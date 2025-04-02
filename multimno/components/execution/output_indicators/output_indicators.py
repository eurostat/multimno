"""

"""

import datetime as dt
import calendar as cal
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType


from multimno.components.execution.present_population.present_population_estimation import PresentPopulationEstimation
from multimno.components.execution.usual_environment_aggregation.usual_environment_aggregation import (
    UsualEnvironmentAggregation,
)
from multimno.components.execution.internal_migration.internal_migration import InternalMigration
from multimno.components.execution.tourism_statistics.tourism_statistics_calculation import TourismStatisticsCalculation
from multimno.components.execution.tourism_outbound_statistics.tourism_outbound_statistics_calculation import (
    TourismOutboundStatisticsCalculation,
)

from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_data_object import SilverPresentPopulationDataObject
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
from multimno.core.data_objects.bronze.bronze_inbound_estimation_factors_data_object import (
    BronzeInboundEstimationFactorsDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.component import Component
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY, CONFIG_GOLD_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import Seasons
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.log import get_execution_stats
from multimno.core.grid import InspireGridGenerator
from multimno.core.utils import apply_schema_casting


KANONYMITY_TYPES = ["obfuscate", "delete"]

USE_CASE_DATA_OBJECTS = {
    PresentPopulationEstimation.COMPONENT_ID: [
        {
            "input_constructor": SilverPresentPopulationDataObject,
            "output_constructor": SilverPresentPopulationZoneDataObject,
            "input_path_config_key": "present_population_silver",
            "output_path_config_key": "present_population_zone_gold",
            "spatial_agg_columns": [ColNames.population],
            "estimation_columns": [ColNames.population],
            "kanonymity_columns": [ColNames.population],
        },
    ],
    UsualEnvironmentAggregation.COMPONENT_ID: [
        {
            "input_constructor": SilverAggregatedUsualEnvironmentsDataObject,
            "output_constructor": SilverAggregatedUsualEnvironmentsZonesDataObject,
            "input_path_config_key": "aggregated_usual_environments_silver",
            "output_path_config_key": "aggregated_usual_environments_zone_gold",
            "spatial_agg_columns": [ColNames.weighted_device_count],
            "estimation_columns": [ColNames.weighted_device_count],
            "kanonymity_columns": [ColNames.weighted_device_count],
        },
    ],
    InternalMigration.COMPONENT_ID: [
        {
            "input_constructor": SilverInternalMigrationDataObject,
            "output_constructor": SilverInternalMigrationDataObject,
            "input_path_config_key": "internal_migration_silver",
            "output_path_config_key": "internal_migration_gold",
            "estimation_columns": [ColNames.migration],
            "kanonymity_columns": [ColNames.migration],
        },
    ],
    TourismStatisticsCalculation.COMPONENT_ID: [
        {
            "input_constructor": SilverTourismZoneDeparturesNightsSpentDataObject,
            "output_constructor": SilverTourismZoneDeparturesNightsSpentDataObject,
            "input_path_config_key": "tourism_geozone_aggregations_silver",
            "output_path_config_key": "tourism_geozone_aggregations_gold",
            "estimation_columns": [ColNames.nights_spent, ColNames.num_of_departures],
            "kanonymity_columns": [ColNames.num_of_departures],
        },
        {
            "input_constructor": SilverTourismTripAvgDestinationsNightsSpentDataObject,
            "output_constructor": SilverTourismTripAvgDestinationsNightsSpentDataObject,
            "input_path_config_key": "tourism_trip_aggregations_silver",
            "output_path_config_key": "tourism_trip_aggregations_gold",
        },
    ],
    TourismOutboundStatisticsCalculation.COMPONENT_ID: [
        {
            "input_constructor": SilverTourismOutboundNightsSpentDataObject,
            "output_constructor": SilverTourismOutboundNightsSpentDataObject,
            "input_path_config_key": "tourism_outbound_aggregations_silver",
            "output_path_config_key": "tourism_outbound_aggregations_gold",
            "estimation_columns": [ColNames.nights_spent],
        }
    ],
}


class OutputIndicators(Component):
    """ """

    COMPONENT_ID = "OutputIndicators"
    USE_CASES = [
        InternalMigration.COMPONENT_ID,
        PresentPopulationEstimation.COMPONENT_ID,
        UsualEnvironmentAggregation.COMPONENT_ID,
        TourismStatisticsCalculation.COMPONENT_ID,
        TourismOutboundStatisticsCalculation.COMPONENT_ID,
    ]

    def __init__(self, general_config_path, component_config_path):
        super().__init__(general_config_path, component_config_path)

        # self.use_case and self.zoning_dataset have been defined in self.initialize_data_objects()

        self.section = f"{self.COMPONENT_ID}.{self.use_case}"

        # Check incompatibility of reserved datasets with some use cases
        if self.zoning_dataset in ReservedDatasetIDs():
            if self.use_case == InternalMigration.COMPONENT_ID:
                raise ValueError(
                    f"Reserved dataset {self.zoning_dataset} execution not implemented for",
                    InternalMigration.COMPONENT_ID,
                )
            # force level to 1 for reserved datasets
            self.logger.info(f"zoning_dataset_id is {self.zoning_dataset} -- forcing hierarchical levels to `[1]`")
            self.levels = [1]
        elif self.zoning_dataset is None:  # no zoning or level needed, e.g. for outbound use case
            pass
        else:
            levels = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "hierarchical_levels")
            self.levels = list(int(x.strip()) for x in levels.split(","))
            if len(levels) == 0:
                raise ValueError(f"Provide at least one hierarchical level -- encountered an empty list")

        self.deduplication_factor_local = self.config.getfloat(self.COMPONENT_ID, "deduplication_factor_local")
        self.deduplication_factor_default_inbound = self.config.getfloat(
            self.COMPONENT_ID, "deduplication_factor_default_inbound"
        )
        self.mno_to_target_population_factor_local = self.config.getfloat(
            self.COMPONENT_ID, "mno_to_target_population_factor_local"
        )
        self.mno_to_target_population_factor_default_inbound = self.config.getfloat(
            self.COMPONENT_ID, "mno_to_target_population_factor_default_inbound"
        )
        self.k = self.config.getint(self.COMPONENT_ID, "k")
        self.anonymity_type = self.config.get(self.COMPONENT_ID, "anonymity_type")
        if self.anonymity_type not in KANONYMITY_TYPES:
            raise ValueError(f"unknown anonymity type `{self.anonymity_type}` -- must be one of {KANONYMITY_TYPES}")

        # Initialise use-case specific partition/segmentation values read from config
        if self.use_case == InternalMigration.COMPONENT_ID:
            self.init_internal_migration()
        elif self.use_case == PresentPopulationEstimation.COMPONENT_ID:
            self.init_present_population()
        elif self.use_case == UsualEnvironmentAggregation.COMPONENT_ID:
            self.init_usual_environment()
        elif self.use_case == TourismStatisticsCalculation.COMPONENT_ID:
            self.init_inbound_tourism()
        elif self.use_case == TourismOutboundStatisticsCalculation.COMPONENT_ID:
            self.init_outbound_tourism()

        # Grid ID origin
        self.origin = None
        # Working df
        self.dfs = None

    def initalize_data_objects(self):
        self.use_case = self.config.get(self.COMPONENT_ID, "use_case")

        # zoning_dataset is not used in outbound tourism, so it will be None for that use case
        self.zoning_dataset = None
        if self.use_case in [
            InternalMigration.COMPONENT_ID,
            UsualEnvironmentAggregation.COMPONENT_ID,
            PresentPopulationEstimation.COMPONENT_ID,
            TourismStatisticsCalculation.COMPONENT_ID,
        ]:
            self.zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "zoning_dataset_id")

        if self.use_case not in self.USE_CASES:
            raise ValueError(f"Unknown use_case `{self.use_case}` -- must be one of {', '.join(self.USE_CASES)}")

        self.input_data_objects = {}
        self.output_data_objects = {}

        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            input_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, do_info["input_path_config_key"])
            output_do_path = self.config.get(CONFIG_GOLD_PATHS_KEY, do_info["output_path_config_key"])

            if not check_if_data_path_exists(self.spark, input_do_path):
                self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                raise ValueError(f"Invalid path for {do_info['input_constructor'].ID}: {input_do_path}")

            clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            input_do = do_info["input_constructor"](self.spark, input_do_path)
            output_do = do_info["output_constructor"](self.spark, output_do_path)

            self.input_data_objects[input_do.ID] = input_do
            self.output_data_objects[output_do.ID] = output_do

        # Present population and UE aggregation might have INSPIRE 100m or INSPIRE 1km grid as zoning dataset, and they
        # have to be mapped and aggregated for them in this component, so load geozones grid map data object
        if self.use_case in [
            PresentPopulationEstimation.COMPONENT_ID,
            UsualEnvironmentAggregation.COMPONENT_ID,
        ]:
            if self.zoning_dataset not in ReservedDatasetIDs():
                input_zone_grid_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "geozones_grid_map_data_silver")

                if not check_if_data_path_exists(self.spark, input_zone_grid_path):
                    self.logger.warning(f"Expected path {input_zone_grid_path} to exist but it does not")
                    raise ValueError(f"Invalid path for {SilverGeozonesGridMapDataObject.ID}: {input_zone_grid_path}")

                self.input_data_objects[SilverGeozonesGridMapDataObject.ID] = SilverGeozonesGridMapDataObject(
                    self.spark, input_zone_grid_path
                )
            else:
                grid_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver")
                self.input_data_objects[SilverGridDataObject.ID] = SilverGridDataObject(self.spark, grid_path)

        # inbound tourism use case also loads estimation factors (deduplication and mno-to-target-pop factors) for
        # different countries of origin. Missing factors or countries in the data object will use default factors
        # specified in configuration
        if self.use_case == TourismStatisticsCalculation.COMPONENT_ID:
            input_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "inbound_estimation_factors_bronze")
            if check_if_data_path_exists(self.spark, input_path):
                self.input_data_objects[BronzeInboundEstimationFactorsDataObject.ID] = (
                    BronzeInboundEstimationFactorsDataObject(self.spark, input_path)
                )
            else:
                self.logger.warning(
                    f"Could not find inbound estimation factors at {input_path} -- using default factors"
                )

    def _parse_validate_month(self, config_key: str) -> dt.date:
        """Parse and validate month string from config file

        Args:
            config_key (str): config key of the parameter to parse and validate

        Raises:
            ValueError: parameter could not be parsed using YYYY-MM format

        Returns:
            dt.date: first day of the specified month
        """
        config_input = self.config.get(self.section, config_key)
        try:
            out = dt.datetime.strptime(config_input, "%Y-%m").date()
        except ValueError as e:
            err_msg = f"Could not parse parameter {config_key} = `{config_input}` -- expected format: YYYY-MM"
            self.logger.error(err_msg)
            raise ValueError(err_msg) from e
        return out

    def _parse_validate_season(self, config_key: str) -> str:
        """Parse and validate season string from config file

        Args:
            config_key (str): config key of the parameter to parse and validate

        Raises:
            ValueError: parameter was not one of the valid season values

        Returns:
            str: season value
        """
        config_input = self.config.get(self.section, config_key)
        if not Seasons.is_valid_type(config_input):
            err_msg = f"Unknown season {config_input} -- valid values are {Seasons.values()}"
            self.logger.error(err_msg)
            raise ValueError(err_msg)

        return config_input

    def _parse_validate_date(self, config_key: str) -> dt.date:
        """Parse and validate date string from config file

        Args:
            config_key (str): config key of the parameter to parse and validate

        Raises:
            ValueError: could not parse parameter using YYYY-MM-DD format

        Returns:
            dt.date: specified date in the config
        """
        config_input = self.config.get(self.section, config_key)
        try:
            out = dt.datetime.strptime(config_input, "%Y-%m-%d").date()
        except ValueError as e:
            err_msg = f"Could not parse parameter {config_key} = `{config_input}` -- expected format: YYYY-MM-DD"
            self.logger.error(err_msg)
            raise ValueError(err_msg) from e
        return out

    def init_internal_migration(self):
        """
        Initialises parameters to filter internal migration data for this execution
        """
        self.start_date_prev = self._parse_validate_month("start_month_previous")
        self.end_date_prev = self._parse_validate_month("end_month_previous")
        self.end_date_prev = self.end_date_prev + dt.timedelta(
            days=cal.monthrange(self.end_date_prev.year, self.end_date_prev.month)[1] - 1
        )
        self.season_prev = self._parse_validate_season("season_previous")

        self.start_date_new = self._parse_validate_month("start_month_new")
        self.end_date_new = self._parse_validate_month("end_month_new")
        self.end_date_new = self.end_date_new + dt.timedelta(
            days=cal.monthrange(self.end_date_new.year, self.end_date_new.month)[1] - 1
        )
        self.season_new = self._parse_validate_season("season_new")

    def init_present_population(self):
        """
        Initialises parameters to filter present population for this execution
        """
        self.start_date = self._parse_validate_date("start_date")
        self.end_date = self._parse_validate_date("end_date")

    def init_inbound_tourism(self):
        """
        Initialises parameters to filter inbound tourism for this execution
        """
        self.start_month = self._parse_validate_month("start_month")
        self.end_month = self._parse_validate_month("end_month")

    def init_outbound_tourism(self):
        """
        Initialises parameters to filter outbound tourism for this execution
        """
        self.start_month = self._parse_validate_month("start_month")
        self.end_month = self._parse_validate_month("end_month")

    def init_usual_environment(self):
        """
        Initialises parameters to filter usual environment for this execution

        Raises:
            ValueError: If no UE label was specified
        """
        labels = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "labels")
        labels = list(x.strip() for x in labels.split(","))

        if len(labels) == 0:
            raise ValueError(f"Provide at least one usual environment label -- encountered an empty list")

        self.labels = labels
        self.start_date = self._parse_validate_month("start_month")
        self.end_date = self._parse_validate_month("end_month")
        self.end_date = self.end_date + dt.timedelta(
            days=cal.monthrange(self.end_date.year, self.end_date.month)[1] - 1
        )
        self.season = self._parse_validate_season("season")

    def filter_dataframe(self, df: DataFrame) -> DataFrame:
        """Filters dataframe according to the config-specified values for the data objects belonging to a specific
        use case

        Args:
            df (DataFrame): dataframe to be filtered

        Raises:
            NotImplementedError: use case that has not been implemented for this function

        Returns:
            DataFrame: filtered dataframe
        """
        if self.use_case == InternalMigration.COMPONENT_ID:
            df = (
                df.where(F.col(ColNames.dataset_id) == self.zoning_dataset)
                .where(F.col(ColNames.level).isin(self.levels))
                .where(F.col(ColNames.start_date_previous) == self.start_date_prev)
                .where(F.col(ColNames.end_date_previous) == self.end_date_prev)
                .where(F.col(ColNames.season_previous) == self.season_prev)
                .where(F.col(ColNames.start_date_new) == self.start_date_new)
                .where(F.col(ColNames.end_date_new) == self.end_date_new)
                .where(F.col(ColNames.season_new) == self.season_new)
            )
        elif self.use_case == PresentPopulationEstimation.COMPONENT_ID:
            df = df.where(
                F.make_date(ColNames.year, ColNames.month, ColNames.day).between(self.start_date, self.end_date)
            )
        elif self.use_case == UsualEnvironmentAggregation.COMPONENT_ID:
            df = (
                df.where(F.col(ColNames.label).isin(self.labels))
                .where(F.col(ColNames.start_date) == self.start_date)
                .where(F.col(ColNames.end_date) == self.end_date)
                .where(F.col(ColNames.season) == self.season)
            )
        elif self.use_case == TourismStatisticsCalculation.COMPONENT_ID:
            # Same filter applies to both data objects
            df = (
                df.where(F.col(ColNames.dataset_id) == self.zoning_dataset)
                .where(F.col(ColNames.level).isin(self.levels))
                .where(
                    (F.col(ColNames.year) > self.start_month.year)
                    | (
                        (F.col(ColNames.year) == self.start_month.year)
                        & (F.col(ColNames.month) >= self.start_month.month)
                    )
                )
                .where(
                    (F.col(ColNames.year) < self.end_month.year)
                    | ((F.col(ColNames.year) == self.end_month.year) & (F.col(ColNames.month) <= self.end_month.month))
                )
            )
        elif self.use_case == TourismOutboundStatisticsCalculation.COMPONENT_ID:
            df = df.where(
                (F.col(ColNames.year) > self.start_month.year)
                | ((F.col(ColNames.year) == self.start_month.year) & (F.col(ColNames.month) >= self.start_month.month))
            ).where(
                (F.col(ColNames.year) < self.end_month.year)
                | ((F.col(ColNames.year) == self.end_month.year) & (F.col(ColNames.month) <= self.end_month.month))
            )
        else:
            raise NotImplementedError(f"use case {self.use_case}")

        return df

    def spatial_aggregation(self):
        """
        Aggregates the indicators of present population and usual environment aggregation to a zoning custom system,
        to the INSPIRE 1km grid, or keep indicators at the INSPIRE 100m grid, depending on what was specified in
        configuration.

        This is a no-op for internal migration, inbound tourism, and outbound tourism use cases.
        """
        # no-op
        if self.use_case in [
            InternalMigration.COMPONENT_ID,
            TourismStatisticsCalculation.COMPONENT_ID,
            TourismOutboundStatisticsCalculation.COMPONENT_ID,
        ]:
            return

        # If dataset is one of INSPIRE 100m or 1km grid, initialise grid generator object
        if self.zoning_dataset in ReservedDatasetIDs():
            grid_gen = InspireGridGenerator(spark=self.spark)

            # We need to get the origin in order to correctly obtain the INSPIRE IDs
            self.origin = self.input_data_objects[SilverGridDataObject.ID].df.first()["origin"]
        else:  # if not, prepare grid-to-zone dataset
            zoning_df = self.input_data_objects[SilverGeozonesGridMapDataObject.ID].df
            zoning_df = zoning_df.filter(F.col(ColNames.dataset_id) == self.zoning_dataset).select(
                ColNames.grid_id, ColNames.hierarchical_id, ColNames.zone_id, ColNames.dataset_id
            )

        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            dfID = do_info["output_constructor"].ID
            segment_columns = [
                x
                for x in do_info["output_constructor"].AGGREGATION_COLUMNS
                if x not in [ColNames.level, ColNames.dataset_id]
            ]
            value_expressions = [F.sum(F.col(col)).alias(col) for col in do_info["spatial_agg_columns"]]
            df = self.dfs[dfID]

            # INSPIRE 100m grid
            if self.zoning_dataset == ReservedDatasetIDs.INSPIRE_100m:
                # impute dataset ID and hierarchical leve equal to 1
                df = df.withColumn(ColNames.dataset_id, F.lit(ReservedDatasetIDs.INSPIRE_100m)).withColumn(
                    ColNames.level, F.lit(1)
                )
                # convert internal grid ID to INSPIRE specification, and rename column to zone_id
                df = grid_gen.grid_id_to_inspire_id(
                    sdf=df, inspire_resolution=100, grid_id_col=ColNames.grid_id, origin=self.origin
                )
                df = df.withColumnRenamed(ColNames.inspire_id, ColNames.zone_id).drop(ColNames.grid_id)

                self.dfs[dfID] = df
                continue
            # INSPIRE 1km grid
            elif self.zoning_dataset == ReservedDatasetIDs.INSPIRE_1km:
                # Transform to INSPIRE representation
                df = grid_gen.grid_id_to_inspire_id(
                    sdf=df, inspire_resolution=1000, grid_id_col=ColNames.grid_id, origin=self.origin
                )
                df = df.withColumnRenamed(ColNames.inspire_id, ColNames.zone_id).drop(ColNames.grid_id)
                # Aggregate
                agg_df = df.groupBy(*segment_columns).agg(*value_expressions)
                # Add hierarchical level 1 and dataset ID columns
                agg_df = agg_df.withColumn(ColNames.level, F.lit(1)).withColumn(
                    ColNames.dataset_id, F.lit(self.zoning_dataset)
                )

                self.dfs[dfID] = agg_df
                continue
            else:  # Using a non-reserved zoning dataset
                for level in self.levels:
                    # Replace zone_id column, which contains the smallest zone ID of the hierarchy, with the zone
                    # of the specific level. The greater zones correspond to lower values of level, and viceversa
                    # Notice that the coarsest level is 1, not 0. The method `F.element_at` has 1-based indexing,
                    # instead of 0-based indexing, so the first level is 1.
                    zone_to_grid_df = zoning_df.withColumn(
                        ColNames.zone_id, F.element_at(F.split(F.col(ColNames.hierarchical_id), pattern="\\|"), level)
                    )

                    # Join grid IDs with the zone they correspond to, aggregate, and add level and dataset ID columns
                    agg_df = df.join(zone_to_grid_df, on=ColNames.grid_id)
                    agg_df = agg_df.groupBy(*segment_columns).agg(*value_expressions)
                    agg_df = agg_df.withColumn(ColNames.level, F.lit(level)).withColumn(
                        ColNames.dataset_id, F.lit(self.zoning_dataset)
                    )

                    # If computing more than one level, we construct more than one data object, so handle this
                    # appropriately in the self.output_data_objects dictionary.
                    if len(self.levels) > 1:
                        output_do = do_info["output_constructor"](
                            self.spark, self.config.get(CONFIG_GOLD_PATHS_KEY, do_info["output_path_config_key"])
                        )
                        self.dfs[dfID + "_" + str(level)] = agg_df
                        self.output_data_objects[dfID + "_" + str(level)] = output_do
                    else:
                        self.dfs[dfID] = agg_df

                # If computing more than one level, all IDs are of the form `dfID_X` where X is one of the levels.
                # We remove the "original" output DO by the new ones we just created.
                if len(self.levels) > 1:
                    del self.dfs[dfID]
                    del self.output_data_objects[dfID]

    def apply_deduplication_factor(self):
        """Applies the deduplication factor to the appropriate value columns of a dataframe. This consists in
        multiplying these values by a factor in order to take into account the fact that some people might carry more
        than one device.

        Raises:
            NotImplementedError: this function does not implement a specific use case
        """
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            # no-op for some data objects
            if "estimation_columns" not in do_info:
                continue

            dfID = do_info["output_constructor"].ID
            # filter the data objects that correspond to this do_info
            for id_ in self.dfs:
                if not id_.startswith(dfID):
                    continue
                df = self.dfs[id_]

                # These use cases just multiply by the same constant factor
                if self.use_case in [
                    UsualEnvironmentAggregation.COMPONENT_ID,
                    PresentPopulationEstimation.COMPONENT_ID,
                    InternalMigration.COMPONENT_ID,
                    TourismOutboundStatisticsCalculation.COMPONENT_ID,
                ]:
                    for col in do_info["estimation_columns"]:
                        df = df.withColumn(col, F.col(col) * F.lit(self.deduplication_factor_local))

                elif self.use_case == TourismStatisticsCalculation.COMPONENT_ID:
                    # check if we were able to find Inbound Estimation Factors data object in the specified directory
                    if BronzeInboundEstimationFactorsDataObject.ID not in self.input_data_objects:
                        # if no factors DO found, just multiply by default value
                        for col in do_info["estimation_columns"]:
                            df = df.withColumn(col, F.col(col) * self.deduplication_factor_default_inbound)
                        continue

                    # If the DO was loaded, load the data object and rename the country column
                    estimation_factors_df = F.broadcast(
                        self.input_data_objects[BronzeInboundEstimationFactorsDataObject.ID].df
                    )
                    estimation_factors_df = estimation_factors_df.withColumnRenamed(
                        ColNames.iso2, ColNames.country_of_origin
                    )
                    # Left join to get the factors for any countries that appear in the factors DO
                    df = df.join(estimation_factors_df, on=ColNames.country_of_origin, how="left")
                    # For missing factors or countries, impute with default values
                    # we also impute here the mno-to-target-population factor
                    df = df.fillna(
                        {
                            ColNames.deduplication_factor: self.deduplication_factor_default_inbound,
                            ColNames.mno_to_target_population_factor: self.mno_to_target_population_factor_default_inbound,
                        }
                    )
                    # mutiply by deduplication factor
                    for col in do_info["estimation_columns"]:
                        df = df.withColumn(col, F.col(col) * F.col(ColNames.deduplication_factor))

                else:
                    raise NotImplementedError(self.use_case)

                self.dfs[id_] = df

    def apply_mno_to_target_population_factor(self):
        """Apply the MNO-to-target population to the value columns of the data objects of the specified use case. This
        is done to estimate the total target population from the number of devices that this particular MNO has.

        Raises:
            NotImplementedError: this function does not implement a specific use case
        """
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            # no-op for some data objects
            if "estimation_columns" not in do_info:
                continue

            dfID = do_info["output_constructor"].ID
            # filter the data objects that correspond to this do_info
            for id_ in self.dfs:
                if not id_.startswith(dfID):
                    continue
                df = self.dfs[id_]

                # These use cases just multiply by the same constant factor
                if self.use_case in [
                    UsualEnvironmentAggregation.COMPONENT_ID,
                    PresentPopulationEstimation.COMPONENT_ID,
                    InternalMigration.COMPONENT_ID,
                    TourismOutboundStatisticsCalculation.COMPONENT_ID,
                ]:
                    for col in do_info["estimation_columns"]:
                        df = df.withColumn(col, F.col(col) * F.lit(self.mno_to_target_population_factor_local))
                elif self.use_case == TourismStatisticsCalculation.COMPONENT_ID:
                    # If inbound factors loaded, we can multiply by the factor loaded previously
                    if BronzeInboundEstimationFactorsDataObject.ID in self.input_data_objects:
                        for col in do_info["estimation_columns"]:
                            df = df.withColumn(col, F.col(col) * F.col(ColNames.mno_to_target_population_factor))
                    else:  # if not, multiply by default factor
                        for col in do_info["estimation_columns"]:
                            df = df.withColumn(col, F.col(col) * self.deduplication_factor_default_inbound)
                else:
                    raise NotImplementedError(self.use_case)

                # If columns don't exist this is a no-op
                df = df.drop(ColNames.deduplication_factor, ColNames.mno_to_target_population_factor)
                self.dfs[id_] = df

    def apply_anonymity_obfuscation(self):
        """Apply k-anonymity obfuscation to the output data objects. Any value in the specified columns that is strictly
        lower than `k` is replaced by `-1`.
        """
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            # no-op for some use cases
            if "kanonymity_columns" not in do_info:
                continue

            dfID = do_info["output_constructor"].ID
            for id_ in self.dfs:
                if not id_.startswith(dfID):
                    continue
                df = self.dfs[id_]

                for col in do_info["kanonymity_columns"]:
                    df = df.withColumn(
                        col,
                        F.when(
                            F.col(col) < F.lit(self.k),
                            F.lit(-1).cast(do_info["output_constructor"].SCHEMA[col].dataType),
                        ).otherwise(F.col(col)),
                    )

                self.dfs[id_] = df

    def apply_anonymity_deletion(self):
        """Apply k-anonymity deletion to the output data objects.  Any value in the specified columns that is strictly
        lower than `k` is removed along with its corresponding row.
        """
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            # no-op for some data objects
            if "kanonymity_columns" not in do_info:
                continue
            dfID = do_info["output_constructor"].ID
            # filter the data objects that correspond to this do_info
            for id_ in self.dfs:
                if not id_.startswith(dfID):
                    continue
                df = self.dfs[id_]

                for col in do_info["kanonymity_columns"]:
                    df = df.where(F.col(col) >= F.lit(self.k))

                self.dfs[id_] = df

    def transform(self):
        # Dictionary that will contain the dataframes as we modify them
        self.dfs = {}
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            input_do_ID = do_info["input_constructor"].ID
            output_do_ID = do_info["output_constructor"].ID
            self.dfs[output_do_ID] = self.filter_dataframe(self.input_data_objects[input_do_ID].df)

        # Spatial aggregation
        self.spatial_aggregation()

        # Deduplication factor
        self.apply_deduplication_factor()

        # Device to target population
        self.apply_mno_to_target_population_factor()

        # Apply k-anonymity
        if self.anonymity_type == "obfuscate":
            self.apply_anonymity_obfuscation()
        elif self.anonymity_type == "delete":
            self.apply_anonymity_deletion()
        else:
            raise ValueError(f"Unknown anonymity type {self.anonymity_type}")  # should not happen

        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            output_do_ID = do_info["output_constructor"].ID

            for id_ in self.dfs:
                if not id_.startswith(output_do_ID):
                    continue

                self.output_data_objects[id_].df = apply_schema_casting(
                    self.dfs[id_], do_info["output_constructor"].SCHEMA
                )

    def check_output_format(self):
        for id_, output_do in self.output_data_objects.items():
            self.logger.info(f"Checking output data object {id_}...")
            output_do.read()

            df = output_do.df
            schema = output_do.SCHEMA

            if len(df.schema) != len(schema):
                self.logger.warning(f"Dataset schema has `{len(df.schema)}` fields -- expected {len(schema)}")

            check_for_nulls = []

            for i, (df_field, expected_field) in enumerate(zip(df.schema, schema)):
                if df_field.name != expected_field.name:
                    self.logger.warning(
                        f"Dataset field number {i+1} has name {df_field.name} -- expected {expected_field.name}"
                    )

                if df_field.dataType != expected_field.dataType:
                    self.logger.warning(
                        f"Dataset field {df_field.name} has type {df_field.dataType} -- expected {expected_field.dataType}"
                    )

                if not expected_field.nullable:
                    check_for_nulls.append(expected_field.name)

            # Count null values
            if check_for_nulls:
                null_counts = (
                    df.select([F.count(F.when(F.isnull(c), True)).alias(c) for c in check_for_nulls])
                    .collect()[0]
                    .asDict()
                )

                columns_with_nulls = {col: count for col, count in null_counts.items() if count > 0}
                if columns_with_nulls:
                    self.logger.warning(f"Unexpected null values detected: {columns_with_nulls}")

            self.logger.info(f"... checked {id_}")

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID} for {self.use_case} data...")

        self.read()
        self.transform()
        self.write()

        self.logger.info(f"Finished writing output of {self.COMPONENT_ID}!")
        self.logger.info(f"Checking fields and format of output data objects...")
        self.check_output_format()
        self.logger.info("Finished!")
