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

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.component import Component
from multimno.core.settings import CONFIG_GOLD_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import SEASONS
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


CLASS_MAPPING = {
    PresentPopulationEstimation.COMPONENT_ID: {
        "constructor": SilverPresentPopulationZoneDataObject,
        "input_path_config_key": "single_mno_{}_present_population_zone_gold".format,
        "output_path_config_key": "multimno_aggregated_present_population_zone_gold",
        "target_column": ColNames.population,
    },
    UsualEnvironmentAggregation.COMPONENT_ID: {
        "constructor": SilverAggregatedUsualEnvironmentsZonesDataObject,
        "input_path_config_key": "single_mno_{}_usual_environment_zone_gold".format,
        "output_path_config_key": "multimno_aggregated_usual_environment_zone_gold",
        "target_column": ColNames.weighted_device_count,
    },
}


class MultiMNOAggregation(Component):
    """
    Class responsible for the aggregation of indicators computed in different MNOs into a single, aggregate MultiMNO
    indicator.
    """

    COMPONENT_ID = "MultiMNOAggregation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.current_component_id: str = None
        self.target_column: str = None
        self.single_mno_factors: list[float] = None
        self.number_of_single_mnos: int = None

    def initalize_data_objects(self):

        self.execute_present_population = self.config.getboolean(self.COMPONENT_ID, "present_population_execution")
        self.execute_usual_environment = self.config.getboolean(self.COMPONENT_ID, "usual_environment_execution")

        self.data_objects = {}

        if self.execute_present_population:
            number_of_single_mnos = self.config.getint(
                f"{self.COMPONENT_ID}.{PresentPopulationEstimation.COMPONENT_ID}", "number_of_single_mnos"
            )
            input_do_paths = [
                self.config.get(
                    CONFIG_GOLD_PATHS_KEY,
                    CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["input_path_config_key"](i),
                )
                for i in range(1, number_of_single_mnos + 1)
            ]
            output_do_path = self.config.get(
                CONFIG_GOLD_PATHS_KEY,
                CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["output_path_config_key"],
            )

            for i, input_do_path in enumerate(input_do_paths):
                if not check_if_data_path_exists(self.spark, input_do_path):
                    self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                    raise ValueError(
                        f"Invalid path for {CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]['constructor'].ID}: {input_do_path}"
                    )
            input_dos = [
                CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["constructor"](self.spark, path)
                for path in input_do_paths
            ]

            clear_destination_directory = self.config.getboolean(
                f"{self.COMPONENT_ID}.PresentPopulationEstimation", "clear_destination_directory"
            )
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            output_do = CLASS_MAPPING[PresentPopulationEstimation.COMPONENT_ID]["constructor"](
                self.spark, output_do_path
            )

            self.data_objects[PresentPopulationEstimation.COMPONENT_ID] = {"input": input_dos, "output": output_do}

        if self.execute_usual_environment:
            number_of_single_mnos = self.config.getint(
                f"{self.COMPONENT_ID}.{UsualEnvironmentAggregation.COMPONENT_ID}", "number_of_single_mnos"
            )
            input_do_paths = [
                self.config.get(
                    CONFIG_GOLD_PATHS_KEY,
                    CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["input_path_config_key"](i),
                )
                for i in range(1, number_of_single_mnos + 1)
            ]
            output_do_path = self.config.get(
                CONFIG_GOLD_PATHS_KEY,
                CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["output_path_config_key"],
            )

            for i, input_do_path in enumerate(input_do_paths):
                if not check_if_data_path_exists(self.spark, input_do_path):
                    self.logger.warning(f"Expected path {input_do_path} to exist but it does not")
                    raise ValueError(
                        f"Invalid path for {CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]['constructor'].ID}: {input_do_path}"
                    )
            input_dos = [
                CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["constructor"](self.spark, path)
                for path in input_do_paths
            ]

            clear_destination_directory = self.config.getboolean(
                f"{self.COMPONENT_ID}.UsualEnvironmentAggregation", "clear_destination_directory"
            )
            if clear_destination_directory:
                delete_file_or_folder(self.spark, output_do_path)

            output_do = CLASS_MAPPING[UsualEnvironmentAggregation.COMPONENT_ID]["constructor"](
                self.spark, output_do_path
            )

            self.data_objects[UsualEnvironmentAggregation.COMPONENT_ID] = {"input": input_dos, "output": output_do}

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

        return df

    @staticmethod
    def filter_out_obfuscated_records(dfs: list[DataFrame], target_column: str) -> list[DataFrame]:
        """Filter out obfuscated records, which are records flagged by the k-anonimity process to have values lower
        than k, for some value k. Specific treatment for these obfuscated values is not yet defined, so we simply
        remove them in this version of the code.

        Args:
            dfs (list[DataFrame]): list of single MNO dataframes with possibly obfuscated values
            target_column (str): name of the target column containig the value of the indicator

        Returns:
            list[DataFrame]: list of single MNO dataframes without obfuscated values
        """
        output_dfs = [df.where(F.col(target_column) > F.lit(0)) for df in dfs]
        return output_dfs

    def aggregate_single_mno_indicators(self, dfs: list[DataFrame]) -> DataFrame:
        """Aggregate the indicator over all single MNOs' data. First, the value of each MNO is multiplied by some
        weight, and then these values are added up, such that the sum of these weights add up to 1.

        Args:
            dfs (list[DataFrame]): list of single MNO dataframes

        Returns:
            DataFrame: final dataframe with the weighted sum indicator of all MNOs
        """
        weighted_dfs = [
            df.withColumn(self.target_column, F.col(self.target_column) * F.lit(factor))
            for (df, factor) in zip(dfs, self.single_mno_factors)
        ]

        groupby_cols = [
            column
            for column in CLASS_MAPPING[self.current_component_id]["constructor"].SCHEMA.names
            if column != self.target_column
        ]

        agg_df = (
            reduce(DataFrame.union, weighted_dfs)
            .groupBy(*groupby_cols)
            .agg(F.sum(self.target_column).alias(self.target_column))
        )

        agg_df = apply_schema_casting(agg_df, CLASS_MAPPING[self.current_component_id]["constructor"].SCHEMA)

        return agg_df

    def transform(self):
        # Get list of input single MNO indicators
        single_mno_dfs = [
            self.input_data_objects[f"{CLASS_MAPPING[self.current_component_id]['constructor'].ID}_{i}"].df
            for i in range(1, self.number_of_single_mnos + 1)
        ]

        single_mno_dfs = [self.filter_dataframe(df) for df in single_mno_dfs]

        # Filter out obfuscated records, if they exist. This is a first-version operation where we don't deal with
        # obfuscated values and just ignore them.
        filtered_dfs = self.filter_out_obfuscated_records(single_mno_dfs, self.target_column)

        # Aggregate values across MNOs
        aggregated_df = self.aggregate_single_mno_indicators(filtered_dfs)

        self.output_data_objects[CLASS_MAPPING[self.current_component_id]["constructor"].ID].df = aggregated_df

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

            # Number of MNOs for which we will aggregate data
            self.number_of_single_mnos = self.config.getint(
                f"{self.COMPONENT_ID}.{self.current_component_id}", "number_of_single_mnos"
            )

            # There must be 2 or more MNOs
            if self.number_of_single_mnos <= 1:
                raise ValueError(
                    f"Number of single MNOs to aggregate must be 2 or greater -- got {self.number_of_single_mnos}"
                )

            self.single_mno_factors = [
                self.config.getfloat(f"{self.COMPONENT_ID}.{self.current_component_id}", f"single_mno_{i}_factor")
                for i in range(1, self.number_of_single_mnos + 1)
            ]

            # Sum of weights must add up to 1.
            if sum(self.single_mno_factors) != 1:
                raise ValueError(
                    f"Single MNO factors {self.single_mno_factors} add up to {sum(self.single_mno_factors)} -- must add up to 1"
                )

            self.input_data_objects = {
                f"{input_do.ID}_{i+1}": input_do
                for i, input_do in enumerate(self.data_objects[self.current_component_id]["input"])
            }

            self.output_data_objects = {
                self.data_objects[component_id]["output"].ID: self.data_objects[self.current_component_id]["output"]
            }

            self.read()
            self.transform()
            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")
