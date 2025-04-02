"""
Module that implements the InternalMigration component
"""

import datetime as dt
import calendar as cal
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import SilverGeozonesGridMapDataObject
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import SilverEnrichedGridDataObject
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_internal_migration_quality_metrics_data_object import (
    SilverInternalMigrationQualityMetricsDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.component import Component
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import Seasons
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


class InternalMigration(Component):
    """
    Class responsible for calculating the internal home location changes produced between two long-term periods and for
    a given zoning system. First, devices with a significant change in home location tiles are found. Then, based on
    tile weights, weights for migration between different zones where the home tiles are contained are computed
    for each device, and these weights are summed up to result in a final weighted device count of devices that migrated
    from one zone to another
    """

    COMPONENT_ID = "InternalMigration"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        # Read and validate migration threshold
        self.migration_threshold = self.config.getfloat(self.COMPONENT_ID, "migration_threshold")
        if self.migration_threshold < 0 or self.migration_threshold > 1:
            msg = f"migration_threshold should be a value between 0 and 1, found {self.migration_threshold}"
            self.logger.error(msg)
            raise ValueError(msg)

        # Read zoning dataset and hierarchical levels
        self.zoning_dataset = self.config.get(self.COMPONENT_ID, "zoning_dataset_id")
        levels = self.config.get(self.COMPONENT_ID, "hierarchical_levels")
        try:
            levels = list(int(x.strip()) for x in levels.split(","))
        except BaseException as e:
            msg = f"hierarchical_levels expected a comma-separated list of integers, but found {levels}"
            self.logger.error(msg)
            raise e(msg)
        self.levels = levels

        # Read and validate identifying values for the first period's home labels
        self.start_date_prev = dt.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "start_month_previous"), "%Y-%m"
        ).date()
        end_date_prev = dt.datetime.strptime(self.config.get(self.COMPONENT_ID, "end_month_previous"), "%Y-%m")
        self.end_date_prev = (
            end_date_prev + dt.timedelta(days=cal.monthrange(end_date_prev.year, end_date_prev.month)[1] - 1)
        ).date()
        if self.start_date_prev > self.end_date_prev:
            msg = f"start_month_previous {self.start_date_prev.strftime('%Y-%m')} must be earlier than end_month_previous {end_date_prev.strftime('%Y-%m')}"
            self.logger.error(msg)
            raise ValueError(msg)

        self.season_prev = self.config.get(self.COMPONENT_ID, "season_previous")
        if not Seasons.is_valid_type(self.season_prev):
            msg = f"Unknown season_previous {self.season_prev} -- valid values are {Seasons.values()}"
            self.logger.error(msg)
            raise ValueError(msg)

        # Read and validate identifying values for the second period's home labels
        self.start_date_new = dt.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "start_month_new"), "%Y-%m"
        ).date()
        end_date_new = dt.datetime.strptime(self.config.get(self.COMPONENT_ID, "end_month_new"), "%Y-%m")
        self.end_date_new = (
            end_date_new + dt.timedelta(days=cal.monthrange(end_date_new.year, end_date_new.month)[1] - 1)
        ).date()
        if self.start_date_new > self.end_date_new:
            msg = f"start_month_new {self.start_date_new.strftime('%Y-%m')} must be earlier than end_month_new {end_date_new.strftime('%Y-%m')}"
            self.logger.error(msg)
            raise ValueError(msg)

        self.season_new = self.config.get(self.COMPONENT_ID, "season_new")
        if not Seasons.is_valid_type(self.season_new):
            msg = f"Unknown season_new {self.season_new} -- valid values are {Seasons.values()}"
            self.logger.error(msg)
            raise ValueError(msg)

        self.landuse_weights = self.config.geteval(self.COMPONENT_ID, "landuse_weights")

        # Initialise other variables
        self.current_level: int = None
        self.quality_metrics_computed: bool = False  # quality metrics need only be computed once
        self.prev_home_tiles: DataFrame = None  # reuse dataframe if already computed for one hierarchical level
        self.new_home_tiles: DataFrame = None  # reuse dataframe if already computed for one hierarchical level
        self.migrating_users: DataFrame = None  # reuse dataframe if already computed for one hierarchical level

    def initalize_data_objects(self):
        # Input paths
        input_enriched_grid_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "enriched_grid_data_silver")
        input_grid_map_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "geozones_grid_map_data_silver")
        input_ue_labels_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "aggregated_usual_environments_silver")

        # Output paths
        output_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "internal_migration_silver")
        quality_metrics_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "internal_migration_quality_metrics")

        # Check uniform for getting the grid or the enriched grid data
        self.uniform_tile_weights = self.config.getboolean(self.COMPONENT_ID, "uniform_tile_weights")
        if not self.uniform_tile_weights:
            if not check_if_data_path_exists(self.spark, input_enriched_grid_path):
                self.logger.warning(f"Expected path {input_enriched_grid_path} to exist but it does not")
                raise ValueError(f"Invalid path for {SilverEnrichedGridDataObject.ID}: {input_enriched_grid_path}")

        if not check_if_data_path_exists(self.spark, input_grid_map_path):
            self.logger.warning(f"Expected path {input_grid_map_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverGeozonesGridMapDataObject.ID}: {input_grid_map_path}")

        if not check_if_data_path_exists(self.spark, input_ue_labels_path):
            self.logger.warning(f"Expected path {input_ue_labels_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverUsualEnvironmentLabelsDataObject.ID}: {input_ue_labels_path}")

        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        if clear_destination_directory:
            delete_file_or_folder(self.spark, output_do_path)

        clear_quality_metrics_directory = self.config.getboolean(self.COMPONENT_ID, "clear_quality_metrics_directory")
        if clear_quality_metrics_directory:
            delete_file_or_folder(self.spark, quality_metrics_path)

        ue_labels = SilverUsualEnvironmentLabelsDataObject(self.spark, input_ue_labels_path)
        grid_map_zones = SilverGeozonesGridMapDataObject(self.spark, input_grid_map_path)
        output_do = SilverInternalMigrationDataObject(self.spark, output_do_path)
        quality_metrics_do = SilverInternalMigrationQualityMetricsDataObject(self.spark, quality_metrics_path)

        self.input_data_objects = {
            SilverUsualEnvironmentLabelsDataObject.ID: ue_labels,
            SilverGeozonesGridMapDataObject.ID: grid_map_zones,
        }
        if not self.uniform_tile_weights:
            self.input_data_objects[SilverEnrichedGridDataObject.ID] = SilverEnrichedGridDataObject(
                self.spark, input_enriched_grid_path
            )

        self.output_data_objects = {output_do.ID: output_do, quality_metrics_do.ID: quality_metrics_do}

    def calculate_landuse_tile_weights(
        self,
        enriched_grid_sdf: DataFrame,
        landuse_prior_weights: Dict[str, float],
    ) -> DataFrame:
        """
        Calculates the tile weights based on landuse information in the enriched grid DataFrame.
        """

        grid_sdf = self.input_data_objects[SilverEnrichedGridDataObject.ID].df
        grid_sdf = grid_sdf.select(
            ColNames.grid_id,
            F.col(ColNames.main_landuse_category),
        )

        # Create a DataFrame from the weights dictionary
        weights_list = [(k, float(v)) for k, v in landuse_prior_weights.items()]
        weights_df = self.spark.createDataFrame(weights_list, [ColNames.main_landuse_category, "weight"])
        weighted_df = enriched_grid_sdf.join(weights_df, on=ColNames.main_landuse_category, how="left").withColumn(
            ColNames.tile_weight, F.coalesce(F.col("weight"), F.lit(0.0))
        )  # Default 0.0 for missing weights

        return weighted_df

    @staticmethod
    def get_migrating_users(
        prev_home_tiles: DataFrame, new_home_tiles: DataFrame, migration_threshold: float
    ) -> tuple[DataFrame, DataFrame]:
        """Computes the overlap index for each device in order to determine if it will be used as a migrating device
        or not. This method also computes the number of unique devices that have home tiles in the first long-term
        period, in the second long-term period (quality metrics).

        Args:
            prev_home_tiles (DataFrame): home tiles of devices in the first long-term period
            new_home_tiles (DataFrame): home tiles of devices in the second long-term period
            migration_threshold (float): threshold to classify a device as migrating or not

        Returns:
            tuple[DataFrame, DataFrame]:
                - First dataframe is the list of devices that have been classified as migrating
                - Second dataframe contains the quality metrics
        """
        prev_home_tiles_alias = prev_home_tiles.alias("prev")
        new_home_tiles_alias = new_home_tiles.alias("new")

        joint_home_tiles = (
            prev_home_tiles_alias.join(
                new_home_tiles_alias,
                how="full",
                on=(
                    (prev_home_tiles_alias[ColNames.user_id_modulo] == new_home_tiles_alias[ColNames.user_id_modulo])
                    & (prev_home_tiles_alias[ColNames.user_id] == new_home_tiles_alias[ColNames.user_id])
                    & (prev_home_tiles_alias[ColNames.grid_id] == new_home_tiles_alias[ColNames.grid_id])
                ),
            )
            .withColumn("coalesced_user_id", F.coalesce("prev." + ColNames.user_id, "new." + ColNames.user_id))
            .withColumn(
                "coalesced_modulo",
                F.coalesce("prev." + ColNames.user_id_modulo, "new." + ColNames.user_id_modulo),
            )
        )
        joint_home_tiles.cache()

        quality_metrics = joint_home_tiles.groupBy().agg(
            F.count_distinct("prev." + ColNames.user_id).alias("previous_home_users"),  # users in first LT
            F.count_distinct("new." + ColNames.user_id).alias("new_home_users"),  # users in second LT
            F.count_distinct("coalesced_user_id").alias("common_home_users"),  # users in both LTs
        )

        migrating_users = (
            joint_home_tiles.groupBy("coalesced_modulo", "coalesced_user_id")
            .agg(
                (F.count("prev." + ColNames.grid_id) + F.count("new." + ColNames.grid_id)).alias("total_count"),
                F.count(
                    F.when(
                        F.col("prev." + ColNames.grid_id) == F.col("new." + ColNames.grid_id),
                        F.col("prev." + ColNames.grid_id),
                    )
                ).alias("common_count"),
            )
            .select(
                F.col("coalesced_modulo").alias(ColNames.user_id_modulo),
                F.col("coalesced_user_id").alias(ColNames.user_id),
                (F.lit(2) * F.col("common_count") / F.col("total_count")).alias("overlap_index"),
            )
            .where(F.col("overlap_index") < migration_threshold)
            .select(ColNames.user_id_modulo, ColNames.user_id)
        )
        migrating_users.cache()

        return migrating_users, quality_metrics

    def get_zone_home_weights(
        self, migrating_prev_home_tiles: DataFrame, migrating_new_home_tiles: DataFrame
    ) -> tuple[DataFrame, DataFrame]:
        """Computes the home weight that each user has from a set of zones to a different set of zones, based
        on the weights assigned to its home tiles.

        Args:
            migrating_prev_home_tiles (DataFrame): home tiles of the first long-term period mapped to their resp. zones
            migrating_new_home_tiles (DataFrame): home tiles of the second long-term period mapped to their resp. zones

        Returns:
            tuple[DataFrame, DataFrame]: dataframes that contain the device-level home weights for the first and
                second long-term periods respectively.
        """
        # Compute tile weight of each user-tile, and add up weight to zones
        window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id)

        # If we are not using uniform weights, get land-use or prior probabilities to use as tile weights
        if not self.uniform_tile_weights:
            grid_sdf = self.input_data_objects[SilverEnrichedGridDataObject.ID].df
            grid_sdf = self.calculate_landuse_tile_weights(grid_sdf, self.landuse_weights)
            grid_sdf = grid_sdf.select(ColNames.grid_id, ColNames.tile_weight)
            prev_weights = migrating_prev_home_tiles.join(grid_sdf, on=ColNames.grid_id, how="inner").withColumn(
                ColNames.tile_weight, ColNames.tile_weight / F.sum(ColNames.tile_weight).over(window)
            )

            new_weights = migrating_new_home_tiles.join(grid_sdf, on=ColNames.grid_id, how="inner").withColumn(
                ColNames.tile_weight, ColNames.tile_weight / F.sum(ColNames.tile_weight).over(window)
            )
        else:  # using uniform tile weights
            prev_weights = migrating_prev_home_tiles.withColumn(
                ColNames.tile_weight, F.lit(1) / F.count("*").over(window)
            )

            new_weights = migrating_new_home_tiles.withColumn(
                ColNames.tile_weight, F.lit(1) / F.count("*").over(window)
            )

        prev_zones = (
            prev_weights.groupBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.zone_id)
            .agg(F.sum(ColNames.tile_weight).alias("prev_weight"))
            .withColumnRenamed(ColNames.zone_id, ColNames.previous_zone)
        )
        new_zones = (
            new_weights.groupBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.zone_id)
            .agg(F.sum(ColNames.tile_weight).alias("new_weight"))
            .withColumnRenamed(ColNames.zone_id, ColNames.new_zone)
        )
        return prev_zones, new_zones

    def transform(self):
        grid_to_zone = self.input_data_objects[SilverGeozonesGridMapDataObject.ID].df
        zone_to_grid_map_sdf = grid_to_zone.withColumn(
            ColNames.zone_id,
            F.element_at(F.split(F.col(ColNames.hierarchical_id), pattern="\\|"), self.current_level),
        )

        # Read home labels for first long-term period
        if self.prev_home_tiles is None:
            self.prev_home_tiles = (
                self.input_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
                .df.where(F.col(ColNames.start_date) == F.lit(self.start_date_prev))
                .where(F.col(ColNames.end_date) == F.lit(self.end_date_prev))
                .where(F.col(ColNames.season) == F.lit(self.season_prev))
                .where(F.col(ColNames.label) == F.lit("home"))
                .select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)
            )
            self.prev_home_tiles.cache()
        # Read home labels for second long-term period
        if self.new_home_tiles is None:
            self.new_home_tiles = (
                self.input_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
                .df.where(F.col(ColNames.start_date) == F.lit(self.start_date_new))
                .where(F.col(ColNames.end_date) == F.lit(self.end_date_new))
                .where(F.col(ColNames.season) == F.lit(self.season_new))
                .where(F.col(ColNames.label) == F.lit("home"))
                .select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)
            )
            self.new_home_tiles.cache()

        # Get list of users that are under the migration threshold, plus quality metrics
        if self.migrating_users is None:
            self.migrating_users, quality_metrics = self.get_migrating_users(
                self.prev_home_tiles, self.new_home_tiles, self.migration_threshold
            )
            self.migrating_users.cache()

        # Join with list of migrating users, and then join with the grid-to-zone mapping
        migrating_prev_home_tiles = (
            self.prev_home_tiles.withColumnRenamed("prev_grid_id", ColNames.grid_id)
            .join(self.migrating_users, on=[ColNames.user_id_modulo, ColNames.user_id])
            .join(zone_to_grid_map_sdf, on=ColNames.grid_id)
        )
        migrating_new_home_tiles = (
            self.new_home_tiles.withColumnRenamed("new_grid_id", ColNames.grid_id)
            .join(self.migrating_users, on=[ColNames.user_id_modulo, ColNames.user_id])
            .join(zone_to_grid_map_sdf, on=ColNames.grid_id)
        )

        prev_zone_weights, new_zone_weights = self.get_zone_home_weights(
            migrating_prev_home_tiles, migrating_new_home_tiles
        )

        # Join before and after zones with their weights
        migration = (
            prev_zone_weights.join(new_zone_weights, on=[ColNames.user_id_modulo, ColNames.user_id])
            .where(F.col(ColNames.previous_zone) != F.col(ColNames.new_zone))  # we don't care about same-zone migration
            .withColumn(ColNames.migration, F.col("prev_weight") * F.col("new_weight"))
            .groupBy(ColNames.previous_zone, ColNames.new_zone)
            .agg(F.sum(ColNames.migration).alias(ColNames.migration))
        )

        # Add additional partition key columns
        migration = migration.withColumns(
            {
                ColNames.dataset_id: F.lit(self.zoning_dataset),
                ColNames.level: F.lit(self.current_level),
                ColNames.start_date_previous: F.lit(self.start_date_prev),
                ColNames.end_date_previous: F.lit(self.end_date_prev),
                ColNames.season_previous: F.lit(self.season_prev),
                ColNames.start_date_new: F.lit(self.start_date_new),
                ColNames.end_date_new: F.lit(self.end_date_new),
                ColNames.season_new: F.lit(self.season_new),
            }
        )

        migration = apply_schema_casting(migration, SilverInternalMigrationDataObject.SCHEMA)
        self.output_data_objects[SilverInternalMigrationDataObject.ID].df = migration

        # If quality metrics have not been written yet
        if not self.quality_metrics_computed:
            self.quality_metrics_computed = True
            quality_metrics = quality_metrics.withColumns(
                {
                    ColNames.result_timestamp: F.current_timestamp(),
                    ColNames.dataset_id: F.lit(self.zoning_dataset),
                    ColNames.level: F.lit(self.current_level),
                    ColNames.start_date_previous: F.lit(self.start_date_prev),
                    ColNames.end_date_previous: F.lit(self.end_date_prev),
                    ColNames.season_previous: F.lit(self.season_prev),
                    ColNames.start_date_new: F.lit(self.start_date_new),
                    ColNames.end_date_new: F.lit(self.end_date_new),
                    ColNames.season_new: F.lit(self.season_new),
                }
            )
            quality_metrics = apply_schema_casting(
                quality_metrics, SilverInternalMigrationQualityMetricsDataObject.SCHEMA
            )
            self.output_data_objects[SilverInternalMigrationQualityMetricsDataObject.ID].df = quality_metrics
        else:
            # If it has already been written, we remove the key from the output DO dictionary so it is not written
            # in the self.write() method
            if SilverInternalMigrationQualityMetricsDataObject.ID in self.output_data_objects:
                del self.output_data_objects[SilverInternalMigrationQualityMetricsDataObject.ID]

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        for level in self.levels:
            self.logger.info(f"Starting migration estimation for hierarchical level {level}...")
            self.current_level = level
            self.transform()
            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")
