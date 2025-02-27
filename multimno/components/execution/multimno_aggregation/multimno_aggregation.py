"""
Module that implements the MultiMNO aggregation component
"""

import datetime as dt
import calendar as cal
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.components.execution.present_population.present_population_estimation import PresentPopulationEstimation
from multimno.components.execution.usual_environment_aggregation.usual_environment_aggregation import (
    UsualEnvironmentAggregation,
)
from multimno.components.execution.internal_migration.internal_migration import InternalMigration
from multimno.components.execution.tourism_statistics.tourism_statistics_calculation import TourismStatisticsCalculation
from multimno.components.execution.tourism_outbound_statistics.tourism_outbound_statistics_calculation import (
    TourismOutboundStatisticsCalculation,
)

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.component import Component
from multimno.core.settings import CONFIG_GOLD_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import Seasons
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


USE_CASE_DATA_OBJECTS = {
    PresentPopulationEstimation.COMPONENT_ID: [
        {
            "constructor": SilverPresentPopulationZoneDataObject,
            "input_path_config_key": "single_mno_{}_present_population_zone_gold".format,
            "output_path_config_key": "multimno_aggregated_present_population_zone_gold",
            "target_columns": [ColNames.population],
        },
    ],
    UsualEnvironmentAggregation.COMPONENT_ID: [
        {
            "constructor": SilverAggregatedUsualEnvironmentsZonesDataObject,
            "input_path_config_key": "single_mno_{}_usual_environment_zone_gold".format,
            "output_path_config_key": "multimno_aggregated_usual_environment_zone_gold",
            "target_columns": [ColNames.weighted_device_count],
        },
    ],
    InternalMigration.COMPONENT_ID: [
        {
            "constructor": SilverInternalMigrationDataObject,
            "input_path_config_key": "single_mno_{}_internal_migration_gold".format,
            "output_path_config_key": "multimno_internal_migration_gold",
            "target_columns": [ColNames.migration],
        },
    ],
    TourismStatisticsCalculation.COMPONENT_ID: [
        {
            "constructor": SilverTourismZoneDeparturesNightsSpentDataObject,
            "input_path_config_key": "single_mno_{}_inbound_tourism_zone_aggregations_gold".format,
            "output_path_config_key": "multimno_inbound_tourism_zone_aggregations_gold",
            "target_columns": [ColNames.nights_spent, ColNames.num_of_departures],
        },
    ],
    TourismOutboundStatisticsCalculation.COMPONENT_ID: [
        {
            "constructor": SilverTourismOutboundNightsSpentDataObject,
            "input_path_config_key": "single_mno_{}_outbound_tourism_aggregations_gold".format,
            "output_path_config_key": "multimno_outbound_tourism_aggregations_gold",
            "target_columns": [ColNames.nights_spent],
        },
    ],
}


class MultiMNOAggregation(Component):
    """
    Class responsible for the aggregation of indicators computed in different MNOs into a single, aggregate MultiMNO
    indicator.
    """

    COMPONENT_ID = "MultiMNOAggregation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)
        # self.use_case, self.number_of_single_mnos already defined

        self.single_mno_factors = [
            self.config.getfloat(self.COMPONENT_ID, f"single_mno_{i}_factor")
            for i in range(1, self.number_of_single_mnos + 1)
        ]

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

    def initalize_data_objects(self):
        self.use_case = self.config.get(self.COMPONENT_ID, "use_case")
        self.section = f"{self.COMPONENT_ID}.{self.use_case}"
        self.number_of_single_mnos = self.config.getint(self.COMPONENT_ID, "number_of_single_mnos")

        if self.number_of_single_mnos <= 1:
            raise ValueError(
                f"Number of single MNOs to aggregate must be 2 or greater -- got {self.number_of_single_mnos}"
            )

        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")

        self.input_data_objects = {}
        self.output_data_objects = {}

        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:
            input_do_path_maker = do_info["input_path_config_key"]
            output_do_path = self.config.get(CONFIG_GOLD_PATHS_KEY, do_info["output_path_config_key"])

            for i in range(1, self.number_of_single_mnos + 1):
                input_do_path = self.config.get(CONFIG_GOLD_PATHS_KEY, input_do_path_maker(i))
                if not check_if_data_path_exists(self.spark, input_do_path):
                    self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                    raise ValueError(f"Invalid path for {do_info['constructor'].ID}: {input_do_path}")

                self.input_data_objects[f"{do_info['constructor'].ID}_{str(i)}"] = do_info["constructor"](
                    self.spark, input_do_path
                )

            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            self.output_data_objects[do_info["constructor"].ID] = do_info["constructor"](self.spark, output_do_path)

    def _parse_validate_month(self, config_key: str) -> dt.datetime:
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
            raise e(err_msg)
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
            raise e(err_msg)
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

        self.zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "zoning_dataset_id")

    def init_present_population(self):
        """
        Initialises parameters to filter present population for this execution
        """
        self.start_date = self._parse_validate_date("start_date")
        self.end_date = self._parse_validate_date("end_date")

        self.zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "zoning_dataset_id")

    def init_inbound_tourism(self):
        """
        Initialises parameters to filter inbound tourism for this execution
        """
        self.start_month = self._parse_validate_month("start_month")
        self.end_month = self._parse_validate_month("end_month")

        self.zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "zoning_dataset_id")

    def init_outbound_tourism(self):
        """
        Initialises parameters to filter outbound tourism for this execution
        """
        self.start_month = self._parse_validate_month("start_month")
        self.end_month = self._parse_validate_month("end_month")

        self.zoning_dataset = None

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
        self.season = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "season")
        if not Seasons.is_valid_type(self.season):
            err_msg = f"Unknown season `{self.season}` specified -- must be one of {Seasons.values()}"
            self.logger.error(err_msg)
            raise ValueError(err_msg)

        self.zoning_dataset = self.config.get(f"{self.COMPONENT_ID}.{self.use_case}", "zoning_dataset_id")

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

    @staticmethod
    def filter_out_obfuscated_records(dfs: list[DataFrame], target_columns: list[str]) -> list[DataFrame]:
        """Filter out obfuscated records, which are records flagged by the k-anonymity process to have values lower
        than k, for some value k. Specific treatment for these obfuscated values is not yet defined, so we simply
        remove them in this version of the code.

        Args:
            dfs (list[DataFrame]): list of single MNO dataframes with possibly obfuscated values
            target_column (str): name of the target column containig the value of the indicator

        Returns:
            list[DataFrame]: list of single MNO dataframes without obfuscated values
        """
        output_dfs = []

        for df in dfs:
            for col in target_columns:
                df = df.where(F.col(col) >= F.lit(0))
            output_dfs.append(df)
        return output_dfs

    def aggregate_single_mno_indicators(self, dfs: list[DataFrame], do_info: dict) -> DataFrame:
        """Aggregate the indicator over all single MNOs' data. First, the value of each MNO is multiplied by some
        weight, and then these values are added up.

        Args:
            dfs (list[DataFrame]): list of single MNO dataframes

        Returns:
            DataFrame: final dataframe with the weighted sum indicator of all MNOs
        """
        weighted_dfs = []

        for i, df in enumerate(dfs):
            for col in do_info["target_columns"]:
                df = df.withColumn(col, F.col(col) * F.lit(self.single_mno_factors[i]))
            weighted_dfs.append(df)

        groupby_cols = [
            column for column in do_info["constructor"].SCHEMA.names if column not in do_info["target_columns"]
        ]

        agg_df = (
            reduce(DataFrame.union, weighted_dfs)
            .groupBy(*groupby_cols)
            .agg(*[F.sum(col).alias(col) for col in do_info["target_columns"]])
        )

        agg_df = apply_schema_casting(agg_df, do_info["constructor"].SCHEMA)

        return agg_df

    def transform(self):
        # Get list of input single MNO indicators
        for do_info in USE_CASE_DATA_OBJECTS[self.use_case]:

            single_mno_dfs = [
                self.input_data_objects[f"{do_info['constructor'].ID}_{str(i)}"].df
                for i in range(1, self.number_of_single_mnos + 1)
            ]

            single_mno_dfs = [self.filter_dataframe(df) for df in single_mno_dfs]

            # Filter out obfuscated records, if they exist. This is a first-version operation where we don't deal with
            # obfuscated values and just ignore them.
            filtered_dfs = self.filter_out_obfuscated_records(single_mno_dfs, do_info["target_columns"])

            # Aggregate values across MNOs
            aggregated_df = self.aggregate_single_mno_indicators(filtered_dfs, do_info)

            self.output_data_objects[do_info["constructor"].ID].df = aggregated_df

    def check_output_format(self):
        for id_, output_do in self.output_data_objects.items():
            self.logger.info(f"Checking output data object {id_}...")
            output_do.read()

            df = output_do.df
            schema = output_do.SCHEMA

            if len(df.schema) != len(schema):
                raise ValueError(f"Dataset schema has `{len(df.schema)}` fields -- expected {len(schema)}")

            check_for_nulls = []

            for i, (df_field, expected_field) in enumerate(zip(df.schema, schema)):
                if df_field.name != expected_field.name:
                    raise ValueError(
                        f"Dataset field number {i+1} has name {df_field.name} -- expected {expected_field.name}"
                    )

                if df_field.dataType != expected_field.dataType:
                    raise TypeError(
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
                    raise ValueError(f"Unexpected null values detected: {columns_with_nulls}")

            self.logger.info(f"... verified {id_}")

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
