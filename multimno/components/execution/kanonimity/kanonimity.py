"""
Module that implements the k-anonimity component
"""

import datetime as dt
import calendar as cal
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.components.execution.present_population.present_population_estimation import PresentPopulationEstimation
from multimno.components.execution.usual_environment_aggregation.usual_environment_aggregation import (
    UsualEnvironmentAggregation,
)
from multimno.components.execution.internal_migration.internal_migration import InternalMigration

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.component import Component
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, CONFIG_GOLD_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import SEASONS
from multimno.core.log import get_execution_stats

CLASS_MAPPING = {
    PresentPopulationEstimation.COMPONENT_ID: {
        "constructor": SilverPresentPopulationZoneDataObject,
        "input_path_config_key": "estimated_present_population_zone_silver",
        "output_path_config_key": "kanonimity_present_population_zone_gold",
        "target_column": ColNames.population,
    },
    UsualEnvironmentAggregation.COMPONENT_ID: {
        "constructor": SilverAggregatedUsualEnvironmentsZonesDataObject,
        "input_path_config_key": "estimated_aggregated_usual_environments_zone_silver",
        "output_path_config_key": "kanonimity_aggregated_usual_environments_zone_gold",
        "target_column": ColNames.weighted_device_count,
    },
    InternalMigration.COMPONENT_ID: {
        "constructor": SilverInternalMigrationDataObject,
        "input_path_config_key": "estimated_internal_migration_silver",
        "output_path_config_key": "kanonimity_internal_migration_gold",
        "target_column": ColNames.migration,
    },
}


class KAnonimity(Component):
    """
    Class responsible for applying k-anonimity of the target column of a specific data object, either by obfuscating
    or deleting rows/registers where the target value is lower than a specified value k.
    """

    COMPONENT_ID = "KAnonimity"
    ANONIMITY_TYPES = ["obfuscate", "delete"]

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.current_component_id: str = None
        self.target_column: str = None
        self.k: int = None
        self.anonimity_type: str = None

    def initalize_data_objects(self):

        self.execute_present_population = self.config.getboolean(self.COMPONENT_ID, "present_population_execution")
        self.execute_usual_environment = self.config.getboolean(self.COMPONENT_ID, "usual_environment_execution")
        self.execute_internal_migration = self.config.getboolean(self.COMPONENT_ID, "internal_migration_execution")

        self.data_objects = {}

        if self.execute_present_population:
            input_do_path = self.config.get(
                CONFIG_SILVER_PATHS_KEY,
                CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["input_path_config_key"],
            )
            output_do_path = self.config.get(
                CONFIG_GOLD_PATHS_KEY,
                CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["output_path_config_key"],
            )

            if not check_if_data_path_exists(self.spark, input_do_path):
                self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                raise ValueError(
                    f"Invalid path for {CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]['constructor'].ID}: {input_do_path}"
                )

            clear_destination_directory = self.config.getboolean(
                f"{self.COMPONENT_ID}.PresentPopulationEstimation", "clear_destination_directory"
            )
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            input_do = CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["constructor"](self.spark, input_do_path)
            output_do = CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["constructor"](
                self.spark, output_do_path
            )

            self.data_objects[PresentPopulationEstimation.COMPONENT_ID] = {"input": input_do, "output": output_do}

        if self.execute_usual_environment:
            input_do_path = self.config.get(
                CONFIG_SILVER_PATHS_KEY,
                CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["input_path_config_key"],
            )
            output_do_path = self.config.get(
                CONFIG_GOLD_PATHS_KEY,
                CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["output_path_config_key"],
            )

            if not check_if_data_path_exists(self.spark, input_do_path):
                self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                raise ValueError(
                    f"Invalid path for {CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]['constructor'].ID}: {input_do_path}"
                )

            clear_destination_directory = self.config.getboolean(
                f"{self.COMPONENT_ID}.UsualEnvironmentAggregation", "clear_destination_directory"
            )
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            input_do = CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["constructor"](self.spark, input_do_path)
            output_do = CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["constructor"](
                self.spark, output_do_path
            )

            self.data_objects[UsualEnvironmentAggregation.COMPONENT_ID] = {"input": input_do, "output": output_do}

        if self.execute_internal_migration:
            input_do_path = self.config.get(
                CONFIG_SILVER_PATHS_KEY,
                CLASS_MAPPING[InternalMigration.COMPONENT_ID]["input_path_config_key"],
            )
            output_do_path = self.config.get(
                CONFIG_GOLD_PATHS_KEY,
                CLASS_MAPPING[InternalMigration.COMPONENT_ID]["output_path_config_key"],
            )

            if not check_if_data_path_exists(self.spark, input_do_path):
                self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                raise ValueError(
                    f"Invalid path for {CLASS_MAPPING[InternalMigration.COMPONENT_ID]['constructor'].ID}: {input_do_path}"
                )

            clear_destination_directory = self.config.getboolean(
                f"{self.COMPONENT_ID}.InternalMigration", "clear_destination_directory"
            )
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            input_do = CLASS_MAPPING[InternalMigration.COMPONENT_ID]["constructor"](self.spark, input_do_path)
            output_do = CLASS_MAPPING[InternalMigration.COMPONENT_ID]["constructor"](self.spark, output_do_path)

            self.data_objects[InternalMigration.COMPONENT_ID] = {"input": input_do, "output": output_do}

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        if len(self.data_objects) == 0:
            self.logger.info("No execution requested in config file -- finishing without performing any operation...")
            self.logger.info(f"Finished {self.COMPONENT_ID}")
            return

        for component_id in self.data_objects:
            self.logger.info(f"Working on {self.COMPONENT_ID}.{component_id}...")
            self.current_component_id = component_id
            self.target_column = CLASS_MAPPING[self.current_component_id]["target_column"]

            self.k = self.config.getint(f"{self.COMPONENT_ID}.{component_id}", "k")
            self.anonimity_type = self.config.get(f"{self.COMPONENT_ID}.{component_id}", "anonimity_type")
            if self.anonimity_type not in self.ANONIMITY_TYPES:
                raise ValueError(
                    f"unknown anonimity type `{self.anonimity_type}` -- must be one of {self.ANONIMITY_TYPES}"
                )

            self.input_data_objects = {
                self.data_objects[component_id]["input"].ID: self.data_objects[component_id]["input"]
            }

            self.output_data_objects = {
                self.data_objects[component_id]["output"].ID: self.data_objects[component_id]["output"]
            }

            self.read()
            self.transform()
            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def filter_dataframe(self, df: DataFrame) -> DataFrame:
        """Filtering function that takes the partitions of the dataframe specified via configuration file

        Args:
            df (DataFrame): original DataFrame

        Raises:
            ValueError: if `season` value in configuration file is not one of allowed values

        Returns:
            DataFrame: filtered DataFrame
        """
        # TODO: move config reading and validation to __init__ (?)
        if self.current_component_id == PresentPopulationEstimation.COMPONENT_ID:
            zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "zoning_dataset_id")
            levels = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "hierarchical_levels")
            levels = list(int(x.strip()) for x in levels.split(","))

            start_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "start_date"), "%Y-%m-%d"
            )
            end_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "end_date"), "%Y-%m-%d"
            )

            df = (
                df.where(F.col(ColNames.dataset_id) == F.lit(zoning_dataset))
                .where(F.col(ColNames.level).isin(levels))
                .where(F.make_date(ColNames.year, ColNames.month, ColNames.day).between(start_date, end_date))
            )

        if self.current_component_id == UsualEnvironmentAggregation.COMPONENT_ID:
            zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "zoning_dataset_id")
            levels = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "hierarchical_levels")
            levels = list(int(x.strip()) for x in levels.split(","))
            labels = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "labels")
            labels = list(x.strip() for x in labels.split(","))

            start_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "start_month"), "%Y-%m"
            )
            end_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "end_month"), "%Y-%m"
            )
            end_date = end_date + dt.timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
            season = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "season")
            if season not in SEASONS:
                raise ValueError(f"Unknown season {season} -- valid values are {SEASONS}")

            df = (
                df.where(F.col(ColNames.dataset_id) == zoning_dataset)
                .where(F.col(ColNames.level).isin(levels))
                .where(F.col(ColNames.label).isin(labels))
                .where(F.col(ColNames.start_date) == start_date)
                .where(F.col(ColNames.end_date) == end_date)
                .where(F.col(ColNames.season) == season)
            )

        if self.current_component_id == InternalMigration.COMPONENT_ID:
            zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "zoning_dataset_id")
            levels = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "hierarchical_levels")
            levels = list(int(x.strip()) for x in levels.split(","))

            start_date_prev = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "start_month_previous"), "%Y-%m"
            )
            end_date_prev = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "end_month_previous"), "%Y-%m"
            )
            end_date_prev = end_date_prev + dt.timedelta(
                days=cal.monthrange(end_date_prev.year, end_date_prev.month)[1] - 1
            )
            season_prev = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "season_previous")
            if season_prev not in SEASONS:
                raise ValueError(f"Unknown season {season_prev} -- valid values are {SEASONS}")

            start_date_new = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "start_month_new"), "%Y-%m"
            )
            end_date_new = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "end_month_new"), "%Y-%m"
            )
            end_date_new = end_date_new + dt.timedelta(
                days=cal.monthrange(end_date_new.year, end_date_new.month)[1] - 1
            )
            season_new = self.config.get(f"{self.COMPONENT_ID}.{self.current_component_id}", "season_new")
            if season_new not in SEASONS:
                raise ValueError(f"Unknown season {season_new} -- valid values are {SEASONS}")

            df = (
                df.where(F.col(ColNames.dataset_id) == zoning_dataset)
                .where(F.col(ColNames.level).isin(levels))
                .where(F.col(ColNames.start_date_previous) == start_date_prev)
                .where(F.col(ColNames.end_date_previous) == end_date_prev)
                .where(F.col(ColNames.season_previous) == season_prev)
                .where(F.col(ColNames.start_date_new) == start_date_new)
                .where(F.col(ColNames.end_date_new) == end_date_new)
                .where(F.col(ColNames.season_new) == season_new)
            )
        return df

    def apply_anonimity_obfuscation(self, df: DataFrame) -> DataFrame:
        """Apply obfuscation at the target column, so all values in the target column that are strictly lower than
        the threshold value k will be replaced by the value `-1`.

        Args:
            df (DataFrame): original DataFrame to perform k-anonimity to

        Returns:
            DataFrame: DataFrame after applying obfuscation
        """
        df = df.withColumn(
            self.target_column,
            F.when(
                F.col(self.target_column) < F.lit(self.k),
                F.lit(-1).cast(
                    CLASS_MAPPING[self.current_component_id]["constructor"].SCHEMA[self.target_column].dataType
                ),
            ).otherwise(F.col(self.target_column)),
        )
        return df

    def apply_anonimity_deletion(self, df: DataFrame) -> DataFrame:
        """Apply deletion at the target column, deleting all rows where the value in the target column is strictly lower
        than the threshold value k.

        Args:
            df (DataFrame): original DataFrame to perform k-anonimity to

        Returns:
            DataFrame: DataFrame after applying deletion
        """
        df = df.where(F.col(self.target_column) >= F.lit(self.k))
        return df

    def transform(self):
        df = self.input_data_objects[CLASS_MAPPING[self.current_component_id]["constructor"].ID].df

        # First, filter based on partition and dates
        df = self.filter_dataframe(df)

        if self.anonimity_type == "obfuscate":
            df = self.apply_anonimity_obfuscation(df)
        elif self.anonimity_type == "delete":
            df = self.apply_anonimity_deletion(df)
        else:
            error_message = (
                f"Unknown anonimity type `{self.anonimity_type}`. Supported types are {self.ANONIMITY_TYPES}"
            )
            self.logger.error(error_message)
            raise ValueError(error_message)

        self.output_data_objects[CLASS_MAPPING[self.current_component_id]["constructor"].ID].df = df
