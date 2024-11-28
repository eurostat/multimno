"""
This module is responsible for usual environment and location labels aggregation to reference grid
"""

from functools import reduce
from typing import Any, Dict
import datetime as dt
import calendar as cal

from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from multimno.core.component import Component
from multimno.core.constants.period_names import SEASONS
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils


class UsualEnvironmentAggregation(Component):
    """
    A class to aggregate devices usual environment and location labels to reference grid.
    """

    COMPONENT_ID = "UsualEnvironmentAggregation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        # Months that compose the long-term period, at least one
        start_month = self.config.get(self.COMPONENT_ID, "start_month")
        try:
            self.start_date = dt.datetime.strptime(start_month, "%Y-%m").date()
        except ValueError as e:
            self.logger.error(f"Could not parse start_month = `{start_month}`. Expected format: YYYY-MM")
            raise e

        end_month = self.config.get(self.COMPONENT_ID, "end_month")
        try:
            self.end_date = dt.datetime.strptime(end_month, "%Y-%m").date()
        except ValueError as e:
            self.logger.error(f"Could not parse end_month = `{end_month}`. Expected format: YYYY-MM")
            raise e
        self.end_date = self.end_date.replace(day=cal.monthrange(self.end_date.year, self.end_date.month)[1])

        if self.end_date < self.start_date:
            raise ValueError(f"End month `{end_month}` should not be an earlier than start month `{start_month}`")

        self.uniform_tile_weights = self.config.getboolean(
            UsualEnvironmentAggregation.COMPONENT_ID, "uniform_tile_weights"
        )

        self.season = self.config.get(self.COMPONENT_ID, "season")
        if self.season not in SEASONS:
            error_msg = f"season: expected one of: {', '.join(SEASONS)} - found: {self.season}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def initalize_data_objects(self):

        # inputs
        self.clear_destination_directory = self.config.getboolean(
            UsualEnvironmentAggregation.COMPONENT_ID, "clear_destination_directory"
        )

        self.input_data_objects = {}
        # Check uniform for getting the grid or the enriched grid data
        uniform_tile_weights = self.config.getboolean(UsualEnvironmentAggregation.COMPONENT_ID, "uniform_tile_weights")

        if uniform_tile_weights:
            inputs = {
                "grid_data_silver": SilverGridDataObject,
                "usual_environment_labels_data_silver": SilverUsualEnvironmentLabelsDataObject,
            }
        else:
            inputs = {
                "enriched_grid_data_silver": SilverEnrichedGridDataObject,
                "usual_environment_labels_data_silver": SilverUsualEnvironmentLabelsDataObject,
            }

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # outputs

        output_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "aggregated_usual_environments_silver")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, output_do_path)

        self.output_data_objects = {}
        self.output_data_objects[SilverAggregatedUsualEnvironmentsDataObject.ID] = (
            SilverAggregatedUsualEnvironmentsDataObject(
                self.spark, output_do_path, [ColNames.start_date, ColNames.end_date, ColNames.season]
            )
        )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        # prepare grid with tile weights
        if self.uniform_tile_weights:
            grid_sdf = self.input_data_objects[SilverGridDataObject.ID].df
        else:
            grid_sdf = self.input_data_objects[SilverEnrichedGridDataObject.ID].df
            grid_sdf = grid_sdf.select(
                ColNames.grid_id,
                ColNames.prior_probability,
            )

        grid_sdf = self.assign_tile_weights(grid_sdf, self.uniform_tile_weights)

        # prepare usual environment labels
        ue_labels_sdf = self.input_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].df
        ue_labels_sdf = ue_labels_sdf.filter(
            (F.col(ColNames.start_date) == F.lit(self.start_date))
            & (F.col(ColNames.end_date) == F.lit(self.end_date))
            & (F.col(ColNames.season) == F.lit(self.season))
        )
        ue_labels_sdf = ue_labels_sdf.select(
            ColNames.user_id, ColNames.grid_id, ColNames.label, ColNames.user_id_modulo
        )
        # aggregate usual environments
        aggregated_ue_sdf = self.aggregate_usual_environments(ue_labels_sdf, grid_sdf)

        # aggreagate location labels
        aggregated_home_labels_sdf = self.aggregate_location_labels(ue_labels_sdf, grid_sdf, "home")

        aggregated_work_labels_sdf = self.aggregate_location_labels(ue_labels_sdf, grid_sdf, "work")

        # union all aggregated results
        aggregated_results_sdf = reduce(
            lambda df1, df2: df1.union(df2), [aggregated_ue_sdf, aggregated_home_labels_sdf, aggregated_work_labels_sdf]
        )

        # Cast column types to DO schema, add missing columns manually
        aggregated_results_sdf = (
            aggregated_results_sdf.withColumn(ColNames.start_date, F.lit(self.start_date))
            .withColumn(ColNames.end_date, F.lit(self.end_date))
            .withColumn(ColNames.season, F.lit(self.season))
        )

        aggregated_results_sdf = utils.apply_schema_casting(
            aggregated_results_sdf, SilverAggregatedUsualEnvironmentsDataObject.SCHEMA
        )

        aggregated_results_sdf = aggregated_results_sdf.repartition(
            *SilverAggregatedUsualEnvironmentsDataObject.PARTITION_COLUMNS
        )

        self.output_data_objects[SilverAggregatedUsualEnvironmentsDataObject.ID].df = aggregated_results_sdf

    def assign_tile_weights(self, grid_sdf: DataFrame, uniform_tile_weights: bool) -> DataFrame:
        """
        Assigns weights to each tile in a DataFrame based on the specified weighting strategy.

        This method updates the input DataFrame by adding a new column that contains the weight of each tile.
        The weighting strategy is determined by the `uniform_tile_weights` parameter. If `uniform_tile_weights` is True,
        all tiles are assigned a uniform weight of 1.0. Otherwise, the tile weights are set to the values derrived from landuse
        information in an input grid data object.

        Parameters:
        - grid_sdf (DataFrame): The input grid DataFrame.
        - uniform_tile_weights (bool): A flag indicating whether to assign uniform weights to all tiles (True)
        or to use the values from grid data object column as weights (False).

        Returns:
        - DataFrame: The updated DataFrame with a new column containing the weights of each tile.
        """
        if uniform_tile_weights:
            grid_sdf = grid_sdf.withColumn(ColNames.tile_weight, F.lit(1.0))
        else:
            grid_sdf = grid_sdf.withColumn(ColNames.tile_weight, F.col(ColNames.prior_probability))

        return grid_sdf

    def get_device_tile_weights(self, ue_labels_sdf: DataFrame, grid_sdf: DataFrame, label: str = "ue") -> DataFrame:
        """
        Calculates and assigns weights to each device UE tiles based on the tile weights in the grid DataFrame.

        This method performs a join operation between ue labels DataFrame and a grid DataFrame based on grid IDs.
        It then calculates the weight of each device tile as the ratio of the tile's weight to the sum of all tile weights
        for the same user for a given label.

        If a label is specified, the method first filters the UE labels DataFrame to include only rows with the matching label
        before proceeding with the join and weight calculation.

        If a label is not specified, the method uses all tiles of a device to calculate the weights.

        Parameters:
        - ue_labels_sdf (DataFrame): The DataFrame containing usual environment and location labels for each device.
        - grid_sdf (DataFrame): The DataFrame containing grid information, including grid IDs and tile weights.
        - label (str, optional): A specific label to filter the UE labels DataFrame.

        Returns:
        - DataFrame: A DataFrame containing the calculated weights for each device tile.
        """

        if label == "ue":
            ue_labels_sdf = ue_labels_sdf.filter(F.col(ColNames.ue_label_rule) != F.lit("ue_na"))

            # as the same tile can be labeled into multiple label categories (e.g. home and work), we need to remove duplicates for ue weights
            ue_labels_sdf = ue_labels_sdf.dropDuplicates([ColNames.user_id, ColNames.grid_id, ColNames.user_id_modulo])
        else:
            ue_labels_sdf = ue_labels_sdf.filter(
                (F.col(ColNames.label) == F.lit(label)) & (F.col(ColNames.location_label_rule) != F.lit("loc_na"))
            )

        ue_labels_sdf = ue_labels_sdf.join(grid_sdf, on=ColNames.grid_id, how="inner")

        window_spec = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id)

        ue_labels_sdf = ue_labels_sdf.withColumn(
            ColNames.device_tile_weight, F.col(ColNames.tile_weight) / F.sum(ColNames.tile_weight).over(window_spec)
        )

        return ue_labels_sdf

    def aggregate_usual_environments(self, ue_labels_sdf: DataFrame, grid_sdf: DataFrame) -> DataFrame:
        """
        Aggregates usual environment by grid ID and calculates the sum of weighted device count.

        This method first calculates device tile weights for usual environment tiles.
        It then aggregates these weights by grid ID to compute the total weighted device count for each grid.
        Finally, it assigns a label "ue" to all aggregated entries to indicate their association with usual environments.

        Parameters:
        - ue_labels_sdf (DataFrame): The DataFrame containing ue labels.
        - grid_sdf (DataFrame): The DataFrame containing grid information with tile weights.

        Returns:
        - DataFrame: A DataFrame with sum of weighted device count per grid tile.
        """
        ue_with_device_weights_sdf = self.get_device_tile_weights(ue_labels_sdf, grid_sdf)

        aggregated_ue_sdf = ue_with_device_weights_sdf.groupBy(ColNames.grid_id).agg(
            F.sum(ColNames.device_tile_weight).alias(ColNames.weighted_device_count)
        )

        aggregated_ue_sdf = aggregated_ue_sdf.withColumn(ColNames.label, F.lit("ue"))

        return aggregated_ue_sdf

    def aggregate_location_labels(self, ue_labels_sdf: DataFrame, grid_sdf: DataFrame, label: str) -> DataFrame:
        """
        Aggregates location labels by grid ID and calculates the sum of weighted device count.

        This method first calculates device tile weights for location label tiles.
        It then aggregates these weights by grid ID to compute the total weighted device count for each grid for each label.
        Finally, it assigns corresponding label name to all aggregated entries to indicate their association with usual environments.

        Parameters:
        - ue_labels_sdf (DataFrame): The DataFrame containing ue labels.
        - grid_sdf (DataFrame): The DataFrame containing grid information with tile weights.

        Returns:
        - DataFrame: A DataFrame with sum of weighted device count per grid tile.
        """

        loc_label_with_device_weights_sdf = self.get_device_tile_weights(ue_labels_sdf, grid_sdf, label)

        aggregated_labels_sdf = loc_label_with_device_weights_sdf.groupBy(ColNames.grid_id).agg(
            F.sum(ColNames.device_tile_weight).alias(ColNames.weighted_device_count)
        )

        aggregated_labels_sdf = aggregated_labels_sdf.withColumn(ColNames.label, F.lit(label))

        return aggregated_labels_sdf
