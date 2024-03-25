"""
Module that calculates cell connection probabilities and posterior probabilities.
"""

import datetime
import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)

from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject

from multimno.core.spark_session import check_if_data_path_exists
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames


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

    def initalize_data_objects(self):

        # Input
        self.input_data_objects = {}
        self.use_land_use_prior = self.config.getboolean(self.COMPONENT_ID, "use_land_use_prior")

        inputs = {
            "cell_footprint_data_silver": SilverCellFootprintDataObject,
        }

        if self.use_land_use_prior:
            inputs["grid_data_silver"] = SilverGridDataObject

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID} in component {self.COMPONENT_ID} initialization")

        # Output
        self.output_data_objects = {}
        self.silver_cell_footprint_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "cell_connection_probabilities_data_silver"
        )
        self.output_data_objects[SilverCellConnectionProbabilitiesDataObject.ID] = (
            SilverCellConnectionProbabilitiesDataObject(
                self.spark,
                self.silver_cell_footprint_path,
                partition_columns=[ColNames.year, ColNames.month, ColNames.day],
            )
        )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # TODO: We might need to iterate over dates and process the data for each date separately
        # have to do tests with larger datasets.

        cell_footprint_df = self.input_data_objects[SilverCellFootprintDataObject.ID].df.filter(
            F.make_date("year", "month", "day").isin(self.data_period_dates)
        )

        # Calculate the cell connection probabilities

        grid_footprint_sums = cell_footprint_df.groupBy(
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.valid_date_start,
            ColNames.valid_date_end,
            ColNames.grid_id,
        ).agg(F.sum(ColNames.signal_dominance).alias("total_grid_footprint"))

        cell_footprint_df = cell_footprint_df.join(
            grid_footprint_sums,
            on=[
                ColNames.year,
                ColNames.month,
                ColNames.day,
                ColNames.valid_date_start,
                ColNames.valid_date_end,
                ColNames.grid_id,
            ],
            how="left",
        )

        cell_conn_probs_df = cell_footprint_df.withColumn(
            ColNames.cell_connection_probability, F.col(ColNames.signal_dominance) / F.col("total_grid_footprint")
        ).drop("total_grid_footprint")

        # Calculate the posterior probabilities

        if self.use_land_use_prior:
            grid_model_df = self.input_data_objects[SilverGridDataObject.ID].df

            # TODO should default value be configurable? 1 would keep cell connection probability value
            # Assign 0 to any missing prior values
            grid_model_df = grid_model_df.fillna(1, subset=[ColNames.prior_probability])

            cell_conn_probs_df = cell_conn_probs_df.join(grid_model_df, on=ColNames.grid_id, how="left")

            cell_conn_probs_df = cell_conn_probs_df.withColumn(
                ColNames.posterior_probability,
                F.col(ColNames.cell_connection_probability) * F.col(ColNames.prior_probability),
            )

        elif not self.use_land_use_prior:
            cell_conn_probs_df = cell_conn_probs_df.withColumn(
                ColNames.posterior_probability, F.col(ColNames.cell_connection_probability)
            )

        # Normalize the posterior probabilities

        total_posterior_df = cell_conn_probs_df.groupBy(ColNames.cell_id).agg(
            F.sum(ColNames.posterior_probability).alias("total_posterior_probability")
        )

        cell_conn_probs_df = (
            cell_conn_probs_df.join(total_posterior_df, on=ColNames.cell_id, how="left")
            .withColumn(
                ColNames.posterior_probability,
                F.col(ColNames.posterior_probability) / F.col("total_posterior_probability"),
            )
            .drop("total_posterior_probability")
        )

        cell_conn_probs_df = cell_conn_probs_df.select(
            *[field.name for field in SilverCellConnectionProbabilitiesDataObject.SCHEMA.fields]
        )
        columns = {
            field.name: F.col(field.name).cast(field.dataType)
            for field in SilverCellConnectionProbabilitiesDataObject.SCHEMA.fields
        }
        cell_conn_probs_df = cell_conn_probs_df.withColumns(columns)

        self.output_data_objects[SilverCellConnectionProbabilitiesDataObject.ID].df = cell_conn_probs_df
