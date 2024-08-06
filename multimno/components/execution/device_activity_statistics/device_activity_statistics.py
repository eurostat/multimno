"""
Module that cleans RAW MNO Event data.
"""

from datetime import datetime, timedelta
import pandas as pd
import pyspark
import pytz

from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql import Window, DataFrame


from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_event_data_object import (
    SilverEventDataObject,
)
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)
from multimno.core.data_objects.silver.silver_device_activity_statistics import (
    SilverDeviceActivityStatistics,
)


from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, TIMEZONE_CONFIG_KEY
from multimno.core.constants.columns import ColNames


class DeviceActivityStatistics(Component):
    """
    Class that removes duplicates from clean MNO Event data
    """

    COMPONENT_ID = "DeviceActivityStatistics"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)
        self.current_date = None
        self.current_input_events = None
        self.current_input_network = None
        self.statistics_df = None

    def initalize_data_objects(self):
        # Input
        # TODO: update this to semantically cleaned files after merge
        self.input_events_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver")
        # TODO: Figure out how this would work with coverage areas. We don't have location of cells in those cases
        self.input_topology_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        self.output_statistics_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "device_activity_statistics")

        self.data_period_start = self.config.get(DeviceActivityStatistics.COMPONENT_ID, "data_period_start")
        self.data_period_end = self.config.get(DeviceActivityStatistics.COMPONENT_ID, "data_period_end")
        self.clear_destination_directory = self.config.get(
            DeviceActivityStatistics.COMPONENT_ID, "clear_destination_directory"
        )
        self.local_timezone_str = self.config.get(TIMEZONE_CONFIG_KEY, "local_timezone")

        # Create all possible dates between start and end
        sdate = pd.to_datetime(self.data_period_start)
        edate = pd.to_datetime(self.data_period_end)
        self.to_process_dates = list(pd.date_range(sdate, edate, freq="d"))

        # Create all input data objects
        self.input_data_objects = {SilverEventDataObject.ID: None}
        if check_if_data_path_exists(self.spark, self.input_events_path):
            # Single input option, with a filter follwed by it
            self.input_data_objects[SilverEventDataObject.ID] = SilverEventDataObject(
                self.spark, self.input_events_path
            )
        else:
            self.logger.warning(f"Expected path {self.input_events_path} to exist but it does not")

        self.input_data_objects[SilverNetworkDataObject.ID] = None

        if check_if_data_path_exists(self.spark, self.input_topology_path):
            # Single input option, with a filter follwed by it
            self.input_data_objects[SilverNetworkDataObject.ID] = SilverNetworkDataObject(
                self.spark, self.input_topology_path
            )
        else:
            self.logger.warning(f"Expected path {self.input_topology_path} to exist but it does not")

        # Output data objects dictionary
        self.output_data_objects = {}
        self.output_data_objects[SilverDeviceActivityStatistics.ID] = SilverDeviceActivityStatistics(
            self.spark, self.output_statistics_path
        )

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, self.output_statistics_path)

        # Create timezones for transformations
        self.local_tz = pytz.timezone(self.local_timezone_str)
        self.utc_tz = pytz.timezone("UTC")

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        self.read()

        for current_date in self.to_process_dates:
            self.current_date = current_date

            start = datetime(year=current_date.year, month=current_date.month, day=current_date.day)
            start_utc = self.local_tz.localize(start).astimezone(self.utc_tz)
            end_utc = start_utc + timedelta(days=1) - timedelta(seconds=1)

            # TODO: should this selection also be based on user_id_modulo?
            self.current_input_events = self.input_data_objects[SilverEventDataObject.ID].df.filter(
                (F.col(ColNames.timestamp).between(start_utc, end_utc))
            )

            self.current_input_network = self.input_data_objects[SilverNetworkDataObject.ID].df.filter(
                (
                    (F.col(ColNames.year) == current_date.year)
                    & (F.col(ColNames.month) == current_date.month)
                    & (F.col(ColNames.day) == current_date.day)
                )
            )

            self.transform()

            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        # Prepare everything for metrics calculations
        df_events = self.preprocess_events(self.current_input_events, self.current_input_network)

        # Calculate count of events per user
        self.statistics_df = df_events.groupby(ColNames.user_id).count().withColumnRenamed("count", ColNames.event_cnt)

        # Calculate count of unique cells per user
        unique_cell_counts = (
            df_events.groupBy(ColNames.user_id)
            .agg(F.countDistinct(ColNames.cell_id))
            .withColumnRenamed(f"count(DISTINCT {ColNames.cell_id})", ColNames.unique_cell_cnt)
        )
        self.statistics_df = self.statistics_df.join(unique_cell_counts, on="user_id")
        # Calculate count of unique locations per user
        unique_location_counts = df_events.groupBy(ColNames.user_id).agg(
            F.countDistinct(ColNames.latitude, ColNames.longitude).alias(ColNames.unique_location_cnt)
        )
        self.statistics_df = self.statistics_df.join(unique_location_counts, on="user_id")

        # Calculate sum of distances between cells
        distance_per_user = df_events.groupBy(ColNames.user_id).agg(
            F.sum("distance").cast(IntegerType()).alias(ColNames.sum_distance_m)
        )
        self.statistics_df = self.statistics_df.join(distance_per_user, on="user_id")

        # Calculate number of unique hours in data per user
        hourly_events_df = df_events.withColumn(
            ColNames.timestamp, F.date_trunc("hour", F.col(ColNames.timestamp))
        ).select([ColNames.user_id, ColNames.timestamp])
        hourly_counts = hourly_events_df.groupBy(ColNames.user_id).agg(
            F.countDistinct(ColNames.timestamp).alias(ColNames.unique_hour_cnt)
        )
        self.statistics_df = self.statistics_df.join(hourly_counts, on="user_id")

        # mean_time_gap
        mean_time_gaps = df_events.groupBy(ColNames.user_id).agg(
            F.mean("time_gap_s").cast(IntegerType()).alias(ColNames.mean_time_gap)
        )
        self.statistics_df = self.statistics_df.join(mean_time_gaps, on="user_id")

        # stdev_time_gap
        stddev_time_gaps = df_events.groupBy(ColNames.user_id).agg(
            F.stddev("time_gap_s").alias(ColNames.stdev_time_gap)
        )
        self.statistics_df = self.statistics_df.join(stddev_time_gaps, on="user_id")
        # Add date to statistics
        self.statistics_df = self.statistics_df.withColumns(
            {
                ColNames.year: F.lit(self.current_date.year).cast("smallint"),
                ColNames.month: F.lit(self.current_date.month).cast("tinyint"),
                ColNames.day: F.lit(self.current_date.day).cast("tinyint"),
            }
        )
        # Reorder rows
        self.statistics_df = self.statistics_df.select([col.name for col in SilverDeviceActivityStatistics.SCHEMA])
        for col in SilverDeviceActivityStatistics.SCHEMA:
            self.statistics_df = self.statistics_df.withColumn(
                col.name, self.statistics_df[col.name].cast(col.dataType)
            )
        self.output_data_objects[SilverDeviceActivityStatistics.ID].df = self.statistics_df

        # after each chunk processing clear all Cache to free memory and disk
        self.spark.catalog.clearCache()

    def preprocess_events(
        self,
        df_events: pyspark.sql.dataframe.DataFrame,
        df_network: pyspark.sql.dataframe.DataFrame,
    ) -> DataFrame:
        """
        Preprocesses events dataframe to be able to calculate all metrics. Steps:
        1. Converts timestamp from UTC to local
        2. Fills latitude and longitude columns from the location of the cell
        3. Gets location of next event for each event
        4. Gets timestamp of next event for each event
        5. Calculates time gap to next event
        6. Calculates distance to next event

        Args:
            df_events (pyspark.sql.dataframe.DataFrame): Events relating to the day that is currently being processed
            df_network (pyspark.sql.dataframe.DataFrame): Network relating to the day that is currently being processed

        Returns:
            df_events: Events relating to the day that is currently being processed with
                extra columns for metric calculation
        """

        # Convert timestamp to local
        df_events.withColumn(
            ColNames.timestamp,
            F.to_utc_timestamp(F.from_utc_timestamp(ColNames.timestamp, self.local_timezone_str), "UTC"),
        )
        df_events = df_events.withColumns(
            {
                ColNames.year: F.year(ColNames.timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.timestamp).cast("tinyint"),
            }
        )

        # Join events with topology data to enable checking unique locations and travelled distances
        df_events = df_events.join(
            df_network.select(
                [
                    F.col(ColNames.cell_id),
                    F.col(ColNames.latitude).alias("cell_lat"),
                    F.col(ColNames.longitude).alias("cell_lon"),
                ]
            ),
            on=ColNames.cell_id,
            how="left",
        )

        # Use latitude and longitude if they exist, otherwise use cells location
        df_events = df_events.withColumn(ColNames.latitude, F.coalesce(ColNames.latitude, "cell_lat"))
        df_events = df_events.withColumn(ColNames.longitude, F.coalesce(ColNames.longitude, "cell_lon"))

        # Add timestamp and location of next record
        window = Window.partitionBy(*[ColNames.user_id_modulo, ColNames.user_id]).orderBy(ColNames.timestamp)
        df_events = df_events.withColumn(
            f"next_{ColNames.timestamp}",
            F.lead(F.col(ColNames.timestamp), 1).over(window),
        )
        df_events = df_events.withColumn(
            f"next_{ColNames.latitude}",
            F.lead(F.col(ColNames.latitude), 1).over(window),
        )
        df_events = df_events.withColumn(
            f"next_{ColNames.longitude}",
            F.lead(F.col(ColNames.longitude), 1).over(window),
        )

        # Calculate time gap to next event
        df_events = df_events.withColumn(
            "time_gap_s",
            F.unix_timestamp(f"next_{ColNames.timestamp}") - F.unix_timestamp(ColNames.timestamp),
        )

        # Calculate the distance between current and next event
        # TODO: check if this is the correct way to calculate, got varying results
        # There are many ways to calculate distance between points and all of them give different results
        df_events = df_events.withColumn(
            "source_geom",
            STF.ST_SetSRID(
                STC.ST_Point(df_events[ColNames.latitude], df_events[ColNames.longitude]),
                4326,
            ),
        )
        df_events = df_events.withColumn(
            "destination_geom",
            STF.ST_SetSRID(
                STC.ST_Point(
                    df_events[f"next_{ColNames.latitude}"],
                    df_events[f"next_{ColNames.longitude}"],
                ),
                4326,
            ),
        )
        df_events = df_events.withColumn(
            "distance",
            STF.ST_DistanceSphere(df_events["source_geom"], df_events["destination_geom"]),
        )

        return df_events
