"""
Module that calculates cell connection probabilities and posterior probabilities.
"""

import datetime
import pyspark.sql.functions as F
from pyspark.sql import Window

from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)

from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats


class CellConnectionProbabilityEstimation(Component):
    """
    Estimates the cell connection probabilities and posterior probabilities for each grid tile.
    Cell connection probabilities are calculated based on footprint per grid.
    Posterior probabilities are calculated based on the cell connection probabilities
    and grid prior probabilities.

    This class reads in cell footprint estimation and the grid model wit prior probabilities.
    The output is a DataFrame that represents cell connection probabilities and
     posterior probabilities for each cell and grid id combination for a given date.
    """

    COMPONENT_ID = "CellConnectionProbabilityEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

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

        self.current_date = None
        self.current_cell_footprint = None
        self.partition_number = self.config.getint(self.COMPONENT_ID, "partition_number")

    def initalize_data_objects(self):

        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")

        # Input
        self.input_data_objects = {}
        self.use_land_use_prior = self.config.getboolean(self.COMPONENT_ID, "use_land_use_prior")

        inputs = {
            "cell_footprint_data_silver": SilverCellFootprintDataObject,
        }

        if self.use_land_use_prior:
            inputs["enriched_grid_data_silver"] = SilverEnrichedGridDataObject

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID} in component {self.COMPONENT_ID} initialization")

        # Output
        self.output_data_objects = {}
        silver_cell_probabilities_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "cell_connection_probabilities_data_silver"
        )

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, silver_cell_probabilities_path)

        self.output_data_objects[SilverCellConnectionProbabilitiesDataObject.ID] = (
            SilverCellConnectionProbabilitiesDataObject(
                self.spark,
                silver_cell_probabilities_path,
            )
        )

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        for current_date in self.data_period_dates:

            self.logger.info(f"Processing cell footprint for {current_date.strftime('%Y-%m-%d')}")

            self.current_date = current_date

            self.current_cell_footprint = self.input_data_objects[SilverCellFootprintDataObject.ID].df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            )

            self.transform()
            self.write()
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        cell_footprint_df = self.current_cell_footprint

        # Calculate the cell connection probabilities

        window_spec = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.grid_id)

        cell_conn_probs_df = cell_footprint_df.withColumn(
            ColNames.cell_connection_probability,
            F.col(ColNames.signal_dominance) / F.sum(ColNames.signal_dominance).over(window_spec),
        )
        # Calculate the posterior probabilities

        if self.use_land_use_prior:
            grid_model_df = self.input_data_objects[SilverEnrichedGridDataObject.ID].df.select(
                ColNames.grid_id, ColNames.prior_probability
            )
            cell_conn_probs_df = cell_conn_probs_df.join(grid_model_df, on=ColNames.grid_id)
            cell_conn_probs_df = cell_conn_probs_df.withColumn(
                ColNames.posterior_probability,
                F.col(ColNames.cell_connection_probability) * F.col(ColNames.prior_probability),
            )

        elif not self.use_land_use_prior:
            cell_conn_probs_df = cell_conn_probs_df.withColumn(
                ColNames.posterior_probability,
                F.col(ColNames.cell_connection_probability),
            )

        # Normalize the posterior probabilities

        window_spec = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id)

        cell_conn_probs_df = cell_conn_probs_df.withColumn(
            ColNames.posterior_probability,
            F.col(ColNames.posterior_probability) / F.sum(ColNames.posterior_probability).over(window_spec),
        )

        cell_conn_probs_df = utils.apply_schema_casting(
            cell_conn_probs_df, SilverCellConnectionProbabilitiesDataObject.SCHEMA
        )
        cell_conn_probs_df = cell_conn_probs_df.coalesce(self.partition_number)

        self.output_data_objects[SilverCellConnectionProbabilitiesDataObject.ID].df = cell_conn_probs_df
