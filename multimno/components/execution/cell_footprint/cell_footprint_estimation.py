"""
Module that cleans RAW MNO Event data.
"""

import datetime
from itertools import combinations
from functools import reduce

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType


from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_signal_strength_data_object import (
    SilverSignalStrengthDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames


class CellFootprintEstimation(Component):
    """
    Estimates the footprint of each cell per grid tile in its coverage area from its signal strength.

    This class reads in signal strength data, calculates the signal dominance of each cell, and prunes the data based on
    configurable thresholds. The output is a DataFrame that represents the footprint of each cell.
    """

    COMPONENT_ID = "CellFootprintEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.logistic_function_steepness = self.config.getfloat(self.COMPONENT_ID, "logistic_function_steepness")
        self.logistic_function_midpoint = self.config.getfloat(self.COMPONENT_ID, "logistic_function_midpoint")
        self.signal_dominance_treshold = self.config.getfloat(self.COMPONENT_ID, "signal_dominance_treshold")
        self.max_cells_per_grid_tile = self.config.getfloat(self.COMPONENT_ID, "max_cells_per_grid_tile")
        self.difference_from_best_sd_treshold = self.config.getfloat(
            self.COMPONENT_ID, "difference_from_best_sd_treshold"
        )
        self.do_sd_treshold_prunning = self.config.getboolean(self.COMPONENT_ID, "do_sd_treshold_prunning")
        self.do_max_cells_per_tile_prunning = self.config.getboolean(
            self.COMPONENT_ID, "do_max_cells_per_tile_prunning"
        )
        self.do_difference_from_best_sd_prunning = self.config.getboolean(
            self.COMPONENT_ID, "do_difference_from_best_sd_prunning"
        )

        self.data_period_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()

        self.data_period_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

    def initalize_data_objects(self):

        self.do_do_cell_intersection_groups_calculation = self.config.getboolean(
            self.COMPONENT_ID, "do_cell_intersection_groups_calculation"
        )
        # Input
        self.input_data_objects = {}

        signal_strength = self.config.get(CONFIG_SILVER_PATHS_KEY, "signal_strength_data_silver")
        if check_if_data_path_exists(self.spark, signal_strength):
            self.input_data_objects[SilverSignalStrengthDataObject.ID] = SilverSignalStrengthDataObject(
                self.spark, signal_strength
            )
        else:
            self.logger.warning(f"Expected path {signal_strength} to exist but it does not")
            raise ValueError(f"Invalid path for {signal_strength} {CellFootprintEstimation.COMPONENT_ID}")

        # Output
        self.output_data_objects = {}
        self.silver_cell_footprint_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        self.output_data_objects[SilverCellFootprintDataObject.ID] = SilverCellFootprintDataObject(
            self.spark,
            self.silver_cell_footprint_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        if self.do_do_cell_intersection_groups_calculation:
            self.silver_cell_intersection_groups_path = self.config.get(
                CONFIG_SILVER_PATHS_KEY, "cell_intersection_groups_data_silver"
            )
            self.output_data_objects[SilverCellIntersectionGroupsDataObject.ID] = (
                SilverCellIntersectionGroupsDataObject(
                    self.spark,
                    self.silver_cell_intersection_groups_path,
                    partition_columns=[ColNames.year, ColNames.month, ColNames.day],
                )
            )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # TODO: We might need to iterate over dates and process the data for each date separately
        # have to do tests with larger datasets.

        current_signal_strength_sdf = self.input_data_objects[SilverSignalStrengthDataObject.ID].df.filter(
            F.make_date("year", "month", "day").isin(self.data_period_dates)
        )

        # Calculate the cell signal dominance
        cell_footprint_sdf = self.signal_strength_to_signal_dominance(
            current_signal_strength_sdf, self.logistic_function_steepness, self.logistic_function_midpoint
        )

        # Prune max cells per grid tile
        if self.do_max_cells_per_tile_prunning:
            cell_footprint_sdf = self.prune_max_cells_per_grid_tile(cell_footprint_sdf, self.max_cells_per_grid_tile)

        # Prune signal dominance difference from best
        if self.do_difference_from_best_sd_prunning:
            cell_footprint_sdf = self.prune_signal_difference_from_best(
                cell_footprint_sdf, self.difference_from_best_sd_treshold
            )

        # Prune small signal dominance values
        if self.do_sd_treshold_prunning:
            cell_footprint_sdf = self.prune_small_signal_dominance(cell_footprint_sdf, self.signal_dominance_treshold)

        cell_footprint_sdf = cell_footprint_sdf.select(SilverCellFootprintDataObject.MANDATORY_COLUMNS)
        columns = {
            field.name: F.col(field.name).cast(field.dataType) for field in SilverCellFootprintDataObject.SCHEMA.fields
        }

        cell_footprint_sdf = cell_footprint_sdf.withColumns(columns)
        self.output_data_objects[SilverCellFootprintDataObject.ID].df = cell_footprint_sdf

        if self.do_do_cell_intersection_groups_calculation:

            cell_intersection_groups_sdf = self.calculate_intersection_groups(cell_footprint_sdf)
            cell_intersection_groups_sdf = self.calculate_all_intersection_combinations(cell_intersection_groups_sdf)

            cell_intersection_groups_sdf = cell_intersection_groups_sdf.select(
                SilverCellIntersectionGroupsDataObject.MANDATORY_COLUMNS
            )
            columns = {
                field.name: F.col(field.name).cast(field.dataType)
                for field in SilverCellIntersectionGroupsDataObject.SCHEMA.fields
            }
            cell_intersection_groups_sdf = cell_intersection_groups_sdf.withColumns(columns)
            self.output_data_objects[SilverCellIntersectionGroupsDataObject.ID].df = cell_intersection_groups_sdf

    @staticmethod
    def signal_strength_to_signal_dominance(
        sdf: DataFrame, logistic_function_steepness: float, logistic_function_midpoint: float
    ) -> DataFrame:
        """
        Converts signal strength to signal dominance using a logistic function.
        Methodology from A Bayesian approach to location estimation of mobile devices
        from mobile network operator data. Tennekes and Gootzen (2022).

        The logistic function is defined as 1 / (1 + exp(-scale)),
        where scale is (signal_strength - logistic_function_midpoint) * logistic_function_steepness.

        Parameters:
        sdf (DataFrame): SignalStrenghtDataObject Spark DataFrame.
        logistic_function_steepness (float): The steepness parameter for the logistic function.
        logistic_function_midpoint (float): The midpoint parameter for the logistic function.

        Returns:
        DataFrame: A Spark DataFrame with the signal dominance added as a new column.
        """
        sdf = sdf.withColumn(
            "scale", (F.col(ColNames.signal_strength) - logistic_function_midpoint) * logistic_function_steepness
        )
        sdf = sdf.withColumn(ColNames.signal_dominance, 1 / (1 + F.exp(-F.col("scale"))))
        sdf = sdf.drop("scale")

        return sdf

    @staticmethod
    def prune_small_signal_dominance(sdf: DataFrame, signal_dominance_threshold: float) -> DataFrame:
        """
        Prunes rows from the DataFrame where the signal dominance is less than or equal to the provided threshold.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        signal_dominance_threshold (float): The threshold for pruning small signal dominance values.

        Returns:
        DataFrame: A DataFrame with rows having signal dominance less than or equal to the threshold removed.
        """
        sdf = sdf.filter(F.col(ColNames.signal_dominance) > signal_dominance_threshold)
        return sdf

    @staticmethod
    def prune_max_cells_per_grid_tile(sdf: DataFrame, max_cells_per_grid_tile: int) -> DataFrame:
        """
        Prunes rows from the DataFrame exceeding the maximum number of cells allowed per grid tile.

        The rows are ordered by signal dominance in descending order,
        and only the top 'max_cells_per_grid_tile' rows are kept for each grid tile.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        max_cells_per_grid_tile (int): The maximum number of cells allowed per grid tile.

        Returns:
        DataFrame: A DataFrame with rows exceeding the maximum number of cells per grid tile removed.
        """
        window = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.grid_id).orderBy(
            F.desc(ColNames.signal_dominance)
        )

        sdf = sdf.withColumn("row_number", F.row_number().over(window))
        sdf = sdf.filter(F.col("row_number") <= max_cells_per_grid_tile)
        sdf = sdf.drop("row_number")

        return sdf

    @staticmethod
    def prune_signal_difference_from_best(sdf: DataFrame, difference_threshold: float) -> DataFrame:
        """
        Prunes rows from the DataFrame based on a threshold of signal dominance difference.

        The rows are ordered by signal dominance in descending order, and only the rows where the difference
        in signal dominance from the maximum is less than the threshold are kept for each grid tile.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        threshold (float): The threshold for signal dominance difference in percentage.

        Returns:
        DataFrame: A DataFrame with rows pruned based on the signal dominance difference threshold.
        """
        window = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.grid_id).orderBy(
            F.desc(ColNames.signal_dominance)
        )

        sdf = sdf.withColumn("row_number", F.row_number().over(window))
        # TODO: Check: Could use F.first instead of F.max as the window is sorted?
        sdf = sdf.withColumn("max_signal_dominance", F.max(ColNames.signal_dominance).over(window))
        sdf = sdf.withColumn(
            "signal_dominance_diff_percentage",
            (sdf["max_signal_dominance"] - sdf[ColNames.signal_dominance]) / sdf["max_signal_dominance"] * 100,
        )

        sdf = sdf.filter(
            (F.col("row_number") == 1) | (F.col("signal_dominance_diff_percentage") <= difference_threshold)
        )

        sdf = sdf.drop("row_number", "max_signal_dominance", "signal_dominance_diff_percentage")

        return sdf

    @staticmethod
    def calculate_intersection_groups(sdf: DataFrame) -> DataFrame:
        """
        Calculates the cell intersection groups based on cell footprints overlaps
        over grid tiles.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.

        Returns:
        DataFrame: A DataFrame with the intersection groups.
        """
        intersections_sdf = sdf.groupBy(
            F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day), F.col(ColNames.grid_id)
        ).agg(F.array_sort(F.collect_set(ColNames.cell_id)).alias(ColNames.cells))

        intersections_sdf = intersections_sdf.withColumn(ColNames.group_size, F.size(F.col(ColNames.cells)))
        intersections_sdf = intersections_sdf.filter(F.col(ColNames.group_size) > 1)

        return intersections_sdf

    @staticmethod
    def calculate_all_intersection_combinations(sdf: DataFrame) -> DataFrame:
        """
        Calculates all possible combinations of intersection groups.
        It is necessary to extract all overlap combinations from every
        intersection group. If there is an intersection group ABC intersection
        groups AB, AC, BC also has to be present.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the intersection groups data.

        Returns:
        DataFrame: A DataFrame with all possible combinations of intersection groups for each grid tile.
        """
        combinations_sdf = []
        max_level = sdf.agg(F.max(ColNames.group_size).alias("max")).collect()[0]["max"]

        for level in range(2, max_level + 1):

            combinations_udf = CellFootprintEstimation.generate_combinations(level)

            combinations_sdf_level = (
                sdf.filter(F.col(ColNames.group_size) >= level)
                .withColumn(ColNames.cells, F.explode(combinations_udf(F.col(ColNames.cells))))
                .select(ColNames.cells, ColNames.year, ColNames.month, ColNames.day)
            )

            combinations_sdf_level = combinations_sdf_level.withColumn(
                ColNames.cells, F.array_sort(F.col(ColNames.cells))
            )
            combinations_sdf_level = combinations_sdf_level.drop_duplicates([ColNames.cells])

            combinations_sdf_level = combinations_sdf_level.withColumn(ColNames.group_size, F.lit(level))

            window = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.group_size).orderBy(
                F.col(ColNames.group_size)
            )

            combinations_sdf_level = combinations_sdf_level.withColumn(
                ColNames.group_id, F.concat(F.col(ColNames.group_size), F.lit("_"), F.row_number().over(window))
            )

            combinations_sdf.append(combinations_sdf_level)

        return reduce(DataFrame.unionAll, combinations_sdf)

    @staticmethod
    def generate_combinations(level):
        def combinations_udf(arr):
            return list(combinations(arr, level))

        return F.udf(combinations_udf, ArrayType(ArrayType(StringType())))
