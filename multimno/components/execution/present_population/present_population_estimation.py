"""Module for estimating the present population of a geographical area at a given time.
"""

import sys
from typing import List, Tuple
from datetime import datetime, timedelta

from pyspark.sql.types import (
    FloatType,
)
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F

from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting
from multimno.core.spark_session import delete_file_or_folder
from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_data_object import (
    SilverPresentPopulationDataObject,
)


class PresentPopulationEstimation(Component):
    """This component calculates the estimated actual population (number of people spatially present)
    for a specified spatial area (country, municipality, grid).

    NOTE: In the current variant 1 of implementation, this module implements only the counting of one
    MNO's users instead of extrapolating to the entire population.
    """

    COMPONENT_ID = "PresentPopulationEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        # Maximum allowed time difference for an event to be included in a time point.
        self.tolerance_period_s = self.config.getint(self.COMPONENT_ID, "tolerance_period_s")

        # Time boundaries for result calculation.
        self.data_period_start = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d %H:%M:%S"
        )
        self.data_period_end = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d  %H:%M:%S"
        )

        # Time gap (time distance in seconds between time points).
        self.time_point_gap_s = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "time_point_gap_s"))

        # Maximum number of iterations for the Bayesian process.
        self.max_iterations = self.config.getint(self.COMPONENT_ID, "max_iterations")

        # Minimum difference threshold between prior and posterior to continue iterating the Bayesian process.
        # Compares sum of absolute differences of each row.
        self.min_difference_threshold = self.config.getfloat(self.COMPONENT_ID, "min_difference_threshold")

        self.event_error_flags_to_include = self.config.geteval(self.COMPONENT_ID, "event_error_flags_to_include")
        self.time_point = None

    def initalize_data_objects(self):
        input_silver_event_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver_flagged")
        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        input_silver_cell_connection_prob_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "cell_connection_probabilities_data_silver"
        )
        input_silver_grid_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver")
        input_silver_event = SilverEventFlaggedDataObject(self.spark, input_silver_event_path)
        input_silver_cell_connection_prob = SilverCellConnectionProbabilitiesDataObject(
            self.spark, input_silver_cell_connection_prob_path
        )
        input_silver_grid = SilverGridDataObject(self.spark, input_silver_grid_path)
        self.input_data_objects = {
            SilverEventFlaggedDataObject.ID: input_silver_event,
            SilverCellConnectionProbabilitiesDataObject.ID: input_silver_cell_connection_prob,
            SilverGridDataObject.ID: input_silver_grid,
        }

        # Output
        # Output data object depends on whether results are aggregated per grid or per zone.
        silver_present_population_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "present_population_silver")
        output_present_population = SilverPresentPopulationDataObject(
            self.spark,
            silver_present_population_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
            mode="append",
        )
        self.output_data_objects = {SilverPresentPopulationDataObject.ID: output_present_population}

        if clear_destination_directory:
            delete_file_or_folder(self.spark, silver_present_population_path)

    @get_execution_stats
    def execute(self):
        self.logger.info("STARTING: Present Population Estimation")

        self.read()

        # Generate desired time points.
        time_points = generate_time_points(self.data_period_start, self.data_period_end, self.time_point_gap_s)

        # Processing logic: handle time points independently one at a time. Write results after each time point.
        for time_point in time_points:
            self.logger.info(f"Present Population: Starting time point {time_point}")
            self.time_point = time_point
            self.transform()
            self.write()
            self.spark.catalog.clearCache()
            self.logger.info(f"Present Population: Finished time point {time_point}")
        # TODO: optimizing when to write.
        # As time points are independent, it seems reasonable to write each out separately.
        # Currently we have hard-coded write mode "overwrite", which does not allow for this,
        # so DO write and pre-write deletion needs handling first.
        self.logger.info("FINISHED: Present Population Estimation")

    def transform(self):
        time_point = self.time_point
        # Filter event data to dates within allowed time bounds.
        events_df = self.input_data_objects[SilverEventFlaggedDataObject.ID].df
        events_df = events_df.filter(F.col(ColNames.error_flag).isin(self.event_error_flags_to_include))

        # Apply date-level filtering to omit events from dates unrelated to this time point.
        events_df = select_where_dates_include_time_point_window(time_point, self.tolerance_period_s, events_df)

        # Number of devices connected to each cell, taking only their event closest to the time_point
        count_per_cell_df = self.calculate_devices_per_cell(events_df, time_point)

        cell_conn_prob_df = self.get_cell_connection_probabilities(time_point)

        # calculate population estimates per grid tile.
        population_per_grid_df = self.calculate_population_per_grid(count_per_cell_df, cell_conn_prob_df)

        # Prepare the results.
        population_per_grid_df = (
            population_per_grid_df.withColumn(ColNames.timestamp, F.lit(time_point))
            .withColumn(ColNames.year, F.lit(time_point.year))
            .withColumn(ColNames.month, F.lit(time_point.month))
            .withColumn(ColNames.day, F.lit(time_point.day))
        )
        # Set results data object
        population_per_grid_df = apply_schema_casting(
            population_per_grid_df, SilverPresentPopulationDataObject.SCHEMA
        )
        self.output_data_objects[SilverPresentPopulationDataObject.ID].df = population_per_grid_df

    def get_cell_connection_probabilities(self, time_point: datetime) -> DataFrame:
        """
        Filter the cell connection probabilities of the dates needed for the time_point provided.
        Args:
            time_point (datetime.datetime): timestamp of time point

        Returns:
            DataFrame: (grid_id, cell_id, cell_connection_probability) dataframe
        """
        time_bound_lower = time_point - timedelta(seconds=self.tolerance_period_s)
        time_bound_upper = time_point + timedelta(seconds=self.tolerance_period_s)

        cell_conn_prob_df = (
            self.input_data_objects[SilverCellConnectionProbabilitiesDataObject.ID]
            .df.select(
                ColNames.year,
                ColNames.month,
                ColNames.day,
                ColNames.cell_id,
                ColNames.grid_id,
                ColNames.cell_connection_probability,
            )
            .filter(
                F.make_date(ColNames.year, ColNames.month, ColNames.day).between(
                    lowerBound=F.lit(time_bound_lower.date()), upperBound=F.lit(time_bound_upper.date())
                )
            )
        )

        return cell_conn_prob_df

    def calculate_devices_per_cell(
        self,
        events_df: DataFrame,
        time_point: datetime,
    ) -> DataFrame:
        """
        Calculates the number of unique users/devices per cell for one time point based on the events inside the
        interval around the time_point. If a device has multiple events inside the interval, the one closest to the
        time_point is selected. In case of a tie, the earliest event is chosen.

        Args:
            events_df (DataFrame): Event data. For each user, expected to contain all of that user's events that can be
                included in this time point.
            time_point (datetime): The timestamp for which the population counts are calculated for.

        Returns:
            DataFrame: Count of devices per cell
        """
        # Filter to include only events within the time window of the time point.
        time_bound_lower = time_point - timedelta(seconds=self.tolerance_period_s)
        time_bound_upper = time_point + timedelta(seconds=self.tolerance_period_s)
        events_df = events_df.where(
            (time_bound_lower <= F.col(ColNames.timestamp)) & (F.col(ColNames.timestamp) <= time_bound_upper)
        )

        # For each user, order events by time distance from time point. If tied, prefer earlier timestamp.
        window = Window.partitionBy(ColNames.user_id).orderBy(
            F.abs(F.col(ColNames.timestamp) - time_point).asc(),
            F.col(ColNames.timestamp),
        )

        events_df = (
            events_df.withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") == F.lit(1))  # event closes to the time_point
            .drop("rank")
        )

        counts_df = events_df.groupBy(ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id).agg(
            F.count(ColNames.user_id).alias(ColNames.device_count)
        )

        return counts_df

    def calculate_population_per_grid(self, devices_per_cell_df: DataFrame, cell_conn_prob_df: DataFrame) -> DataFrame:
        """
        Calculates population estimates for each grid tile Using an iterative Bayesian process.

        Args:
            devices_per_cell_df (DataFrame): (cell_id, device_count) dataframe
            cell_conn_prob_df (DataFrame): (grid_id, cell_id, cell_connection_probability) dataframe
        Returns:
            DataFrame: (grid_id, population) dataframe
        """
        devices_per_cell_df.cache()

        # First, calculate total number of devices and grid tiles to initialise the prior
        total_devices = devices_per_cell_df.select(F.sum(ColNames.device_count).alias("total_devices")).collect()[0][
            "total_devices"
        ]

        if total_devices is None or total_devices == 0:
            total_devices = 0
            # TODO: do not enter iterations, as everything will be zero!

        grid_df = self.input_data_objects[SilverGridDataObject.ID].df.select(ColNames.grid_id)

        # TODO: total number of grid tiles, or total number of grid tiles covered by the cells our events refer to?
        total_tiles = grid_df.count()

        # Initial prior value of population per tile
        initial_prior_value = float(total_devices / total_tiles)

        # Create master dataframe
        # Fields: year, month, day, cell_id, grid_id, cell_connection_probability, device_count(in the cell)
        master_df = cell_conn_prob_df.join(
            devices_per_cell_df, on=[ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id]
        ).withColumn(ColNames.population, F.lit(initial_prior_value))

        niter = 0
        diff = sys.float_info.max

        normalisation_window = Window().partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id)

        master_df.cache()

        # Declare variables here for clarity: they store the result of the previous and the current iterations
        pop_df = None
        new_pop_df = None
        new_master_df = None

        while niter < self.max_iterations and diff >= self.min_difference_threshold:
            # If this is not the first iteration, we need to update the population value used as prior
            if new_pop_df is not None:
                pop_df = new_pop_df

            if pop_df is not None:
                new_master_df = master_df.drop(ColNames.population).join(pop_df, on=ColNames.grid_id)
            else:
                new_master_df = master_df

            new_master_df = (
                new_master_df.withColumn(  # numerator of Bayes' rule
                    ColNames.population, F.col(ColNames.population) * F.col(ColNames.cell_connection_probability)
                )
                # nb of devices in cell i TIMES prob(tile j | cell i)
                # i.e. Devices in cell i * proportion of devices of cell i that belong to tile j
                .withColumn(
                    ColNames.population,
                    (
                        F.col(ColNames.device_count)
                        * F.col(ColNames.population)
                        / F.sum(ColNames.population).over(normalisation_window)
                    ),
                )
            )

            # Compute new population estimation
            new_pop_df = new_master_df.groupby(ColNames.grid_id).agg(
                F.sum(ColNames.population).alias(ColNames.population)
            )

            new_pop_df.cache()

            if pop_df is None:
                diff_df = new_pop_df.select(
                    F.sum(F.abs(F.col(ColNames.population) - F.lit(initial_prior_value))).alias("difference")
                )
            else:
                diff_df = (
                    new_pop_df.withColumnRenamed(ColNames.population, "new_population")
                    .join(pop_df, on=ColNames.grid_id)
                    .select(F.sum(F.abs(F.col(ColNames.population) - F.col("new_population"))).alias("difference"))
                )
            diff = diff_df.collect()[0]["difference"]
            if diff is None:
                diff = 0

            if pop_df is not None:
                pop_df.unpersist()

            niter += 1
            self.logger.info(f"Finished iteration {niter}, diff {diff} vs threshold {self.min_difference_threshold}")

        if diff < self.min_difference_threshold:
            self.logger.info(f"Algorithm convergence for tolerance {self.min_difference_threshold}!")
        else:
            self.logger.info(f"Stopped iterations after reaching max iterations {self.max_iterations}")
        # At the end of the iteration, we have our population estimation over the grid tiles
        return new_pop_df.withColumn(ColNames.population, F.col(ColNames.population).cast(FloatType()))

def generate_time_points(period_start: datetime, period_end: datetime, time_point_gap_s: timedelta) -> List[datetime]:
    """
    Generates time points within the specified period with the specified spacing.

    Args:
        period_start (datetime): Start timestamp of generation.
        period_end (datetime): End timestamp of generation.
        time_point_gap_s (timedelta): Time delta object defining the space between consectuive time points.

    Returns:
        [datetime]: List of time point timestamps.
    """
    # TODO this might be reusable across components.
    time_points = []
    one_time_point = period_start
    while one_time_point <= period_end:
        time_points.append(one_time_point)
        one_time_point = one_time_point + time_point_gap_s
    return time_points


def select_where_dates_include_time_point_window(
    time_point: datetime, tolerance_period_s: int, df: DataFrame
) -> DataFrame:
    """
    Applies filtering to the DataFrame to omit rows from dates which are outside time point boundaries.
    The purpose is to leverage our Parquet partitioning schema (partitioned by year, month, day) and
    use predicate pushdown to avoid reading event data from irrelevant days.

    Args:
        time_point (datetime): Fixed timestamp to calculate results for.
        tolerance_period_s (int): Time window size. Time in seconds before and after the time point
        within which the event data is included.
        df (DataFrame): DataFrame of event data storage partitioned by year, month, day.

    Returns:
        DataFrame: df including only data from dates which include some part of the time point's window.
    """
    time_bound_lower = time_point - timedelta(seconds=tolerance_period_s)
    date_lower = time_bound_lower.date()
    time_bound_upper = time_point + timedelta(seconds=tolerance_period_s)
    date_upper = time_bound_upper.date()
    return df.where(
        (F.make_date(ColNames.year, ColNames.month, ColNames.day) >= date_lower)
        & (F.make_date(ColNames.year, ColNames.month, ColNames.day) <= date_upper)
    )