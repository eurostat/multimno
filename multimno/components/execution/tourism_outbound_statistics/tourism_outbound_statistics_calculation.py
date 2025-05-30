"""
Module that implements the Outbound Tourism use case.
"""

import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from math import ceil

from multimno.core.data_objects.bronze.bronze_mcc_iso_tz_map import BronzeMccIsoTzMap
from multimno.core.data_objects.silver.silver_time_segments_data_object import SilverTimeSegmentsDataObject
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.component import Component
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, CONFIG_BRONZE_PATHS_KEY
from multimno.core.constants.columns import ColNames, SegmentStates
from multimno.core.constants.error_types import UeGridIdType
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting

from pyspark.sql.types import StringType, LongType


class TourismOutboundStatisticsCalculation(Component):
    """
    A class to calculate outbound tourism statistics per time period.
    """

    COMPONENT_ID = "TourismOutboundStatisticsCalculation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.strptime(self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m")
        self.data_period_end = datetime.strptime(self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m")
        # Generate (month_first_date, month_last_date) pairs
        self.data_period_bounds_list = self.get_period_month_bounds_list(self.data_period_start, self.data_period_end)

        self.min_duration_segment_m = self.config.getint(self.COMPONENT_ID, "min_duration_segment_m")
        self.functional_midnight_h = self.config.getint(self.COMPONENT_ID, "functional_midnight_h")
        self.min_duration_segment_night_m = self.config.getint(self.COMPONENT_ID, "min_duration_segment_night_m")

        self.max_trip_gap_h = self.config.getint(self.COMPONENT_ID, "max_outbound_trip_gap_h")

    def initalize_data_objects(self):

        self.filter_ue_segments = self.config.getboolean(self.COMPONENT_ID, "filter_ue_segments")

        # Delete existing trips if needed
        self.delete_existing_trips = self.config.getboolean(self.COMPONENT_ID, "delete_existing_trips")
        if self.delete_existing_trips:
            output_tourism_trip_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_outbound_trips_silver")
            delete_file_or_folder(self.spark, output_tourism_trip_path)
        # Input
        self.input_data_objects = {}

        inputs = {
            "time_segments_silver": SilverTimeSegmentsDataObject,
            "mcc_iso_timezones_data_bronze": BronzeMccIsoTzMap,
            # tourism trips are handled separately
        }

        # If releveant, add usual environment labels data object to filter users with UE labels in target countries.
        if self.filter_ue_segments:
            inputs["usual_environment_labels_data_silver"] = SilverUsualEnvironmentLabelsDataObject

        for key, value in inputs.items():
            if self.config.has_option(CONFIG_BRONZE_PATHS_KEY, key):
                path = self.config.get(CONFIG_BRONZE_PATHS_KEY, key)
            else:
                path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # Separate handling for tourism trips: data existence is optional.
        # If data does not exist, create empty dataframe with proper schema.
        outbound_tourism_trip_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_outbound_trips_silver")
        self.input_data_objects[SilverTourismTripDataObject.ID] = SilverTourismTripDataObject(
            self.spark, outbound_tourism_trip_path
        )
        if not check_if_data_path_exists(self.spark, outbound_tourism_trip_path):
            self.input_data_objects[SilverTourismTripDataObject.ID].df = self.spark.createDataFrame(
                [], schema=SilverTourismTripDataObject.SCHEMA
            )
            self.input_data_objects[SilverTourismTripDataObject.ID].write()  # Write empty dataframe to path

        # Output
        self.output_data_objects = {}

        self.output_tourism_trip_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_outbound_trips_silver")
        self.output_data_objects[SilverTourismTripDataObject.ID] = SilverTourismTripDataObject(
            self.spark,
            self.output_tourism_trip_path,
        )

        self.output_tourism_outbound_aggregation_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "tourism_outbound_aggregations_silver"
        )
        self.output_data_objects[SilverTourismOutboundNightsSpentDataObject.ID] = (
            SilverTourismOutboundNightsSpentDataObject(
                self.spark,
                self.output_tourism_outbound_aggregation_path,
            )
        )

        # Output clearing
        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        if clear_destination_directory:
            delete_file_or_folder(self.spark, self.output_tourism_outbound_aggregation_path)

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        # Handle mcc to iso timezone mapping data.
        self.mcc_iso_tz_map_df = self.input_data_objects[BronzeMccIsoTzMap.ID].df.select(
            ColNames.mcc, ColNames.iso2, ColNames.timezone
        )

        # for every month within the data period, process stays and calculate aggregations.
        for current_month_date_min, current_month_date_max in self.data_period_bounds_list:

            self.read()
            self.current_month_date_min, self.current_month_date_max = current_month_date_min, current_month_date_max
            self.current_month_time_period_string = f"{current_month_date_min.year}-{current_month_date_min.month:0>2}"
            # Increase date_max to include stays from the next month within max_trip_gap_h
            input_date_max = current_month_date_max + timedelta(days=ceil(self.max_trip_gap_h / 24.0))
            input_timestamp_max = current_month_date_max + timedelta(hours=self.max_trip_gap_h)
            self.logger.info(
                f"Processing segments in date range {current_month_date_min.strftime('%Y-%m-%d')} - {input_date_max.strftime('%Y-%m-%d')}, look-forward timestamp max: {input_timestamp_max}"
            )

            # Select segments of the current month plus those within the look-forward window
            current_month_stays_df = (
                self.input_data_objects[SilverTimeSegmentsDataObject.ID]
                .df.filter(
                    (
                        F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                        >= F.lit(current_month_date_min)
                    )
                    & (
                        F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                        <= F.lit(input_date_max)
                    )
                    & (F.col(ColNames.start_timestamp) <= input_timestamp_max)
                    & (F.col(ColNames.state) == SegmentStates.ABROAD)  # Only consider abroad segments
                    & (
                        (F.col(ColNames.end_timestamp).cast("long") - F.col(ColNames.start_timestamp).cast("long"))
                        / 60.0
                        >= self.min_duration_segment_m
                    )  # Filter out segments shorter than min_duration_segment_m
                )
                .select(
                    F.col(ColNames.user_id),
                    F.col(ColNames.time_segment_id),
                    F.lit(None).alias(ColNames.trip_id),
                    F.lit(None).alias(ColNames.trip_start_timestamp),
                    F.col(ColNames.start_timestamp),
                    F.col(ColNames.end_timestamp),
                    F.col(ColNames.user_id_modulo),
                    F.col(ColNames.plmn),
                    F.col(ColNames.year),
                    F.col(ColNames.month),
                )
            )

            # get list of outbound UE from usual environment labels, filter stays to only include users with outbound UE in target countries
            if self.filter_ue_segments:
                outbound_ue_df = (
                    self.input_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
                    .df.filter(
                        ((F.col(ColNames.label) == "ue"))
                        & (F.col(ColNames.start_date) <= current_month_date_min)
                        & (F.col(ColNames.end_date) >= current_month_date_max)
                        & (F.col(ColNames.id_type) == UeGridIdType.ABROAD_STR)
                    )
                    .select(ColNames.user_id, ColNames.grid_id, ColNames.user_id_modulo)
                    .distinct()
                )
                # Filter stays to only include users without outbound UE labels
                current_month_stays_df = current_month_stays_df.join(
                    outbound_ue_df.select(
                        F.col(ColNames.user_id).alias("ue_user_id"),
                        F.col(ColNames.grid_id).alias("ue_mcc"),
                        F.col(ColNames.user_id_modulo).alias("ue_user_id_modulo"),
                    ),
                    (F.col(ColNames.user_id_modulo) == F.col("ue_user_id_modulo"))
                    & (F.col(ColNames.user_id) == F.col("ue_user_id"))
                    & (F.col(ColNames.plmn).substr(1, 3) == F.col("ue_mcc")),
                    "left_anti",
                ).drop("ue_user_id", "ue_mcc", "ue_user_id_modulo")

            # Get previous ongoing trips
            ongoing_trips_df = self.input_data_objects[SilverTourismTripDataObject.ID].df.filter(
                (F.col(ColNames.is_trip_finished) == False)
                & (F.col(ColNames.year) == (current_month_date_min - relativedelta(months=1)).year)
                & (F.col(ColNames.month) == (current_month_date_min - relativedelta(months=1)).month)
            )
            if ongoing_trips_df.count() == 0:
                self.relevant_stays_df = current_month_stays_df
                self.logger.info("No ongoing trips from the previous month.")
            else:
                # get earliest start timestamp of the trip
                earliest_start_timestamp = ongoing_trips_df.select(F.min(ColNames.trip_start_timestamp)).collect()[0][0]

                # get all stays starting from earliest date of ongoing trips
                last_month_stays_df = (
                    self.input_data_objects[SilverTimeSegmentsDataObject.ID]
                    .df.filter(
                        (F.col(ColNames.start_timestamp) >= earliest_start_timestamp)
                        & (F.col(ColNames.end_timestamp) < current_month_date_min)
                        & (F.col(ColNames.state) == SegmentStates.ABROAD)
                    )
                    .select(
                        F.col(ColNames.user_id),
                        F.col(ColNames.time_segment_id),
                        F.col(ColNames.start_timestamp),
                        F.col(ColNames.end_timestamp),
                        F.col(ColNames.user_id_modulo),
                        F.col(ColNames.plmn),
                        F.col(ColNames.year),
                        F.col(ColNames.month),
                    )
                )

                ongoing_trips_stays_df = ongoing_trips_df.withColumn(
                    ColNames.time_segment_id, F.explode(ColNames.time_segment_ids_list)
                ).select(
                    ColNames.user_id,
                    ColNames.trip_id,
                    ColNames.trip_start_timestamp,
                    ColNames.time_segment_id,
                    ColNames.user_id_modulo,
                )

                ongoing_trips_stays_df = ongoing_trips_stays_df.join(
                    last_month_stays_df,
                    [ColNames.user_id, ColNames.time_segment_id, ColNames.user_id_modulo],
                    "inner",
                )
                self.relevant_stays_df = current_month_stays_df.unionByName(ongoing_trips_stays_df)

            self.transform()
            self.write()
            self.spark.catalog.clearCache()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        relevant_stays_df = self.relevant_stays_df
        mcc_iso_tz_map_df = self.mcc_iso_tz_map_df

        # Process abroad segments:
        # 1. Join with MCC_ISO_TZ_MAP to get iso2 and timezone
        # 2. Mark segments as overnight or not based on local time zone

        relevant_stays_df = relevant_stays_df.join(
            mcc_iso_tz_map_df,
            F.col(ColNames.plmn).substr(1, 3) == F.col(ColNames.mcc),
            "left",
        )

        relevant_stays_df = self.mark_overnight_segments(relevant_stays_df)

        # Calculate trips
        relevant_stays_df = self.calculate_trips(relevant_stays_df)
        relevant_stays_df = relevant_stays_df.cache()
        relevant_stays_df.count()
        # NOTE: Count is used to force dataframe calculation to prevent aggregation calculations from reading the
        #  new trips as input due to cache behaviour. Not certain if this is the best solution.

        # group stays to trips collect list of segment ids.
        trips_df = relevant_stays_df.groupBy(ColNames.user_id, ColNames.trip_id).agg(
            F.first(ColNames.is_trip_finished).alias(ColNames.is_trip_finished),
            F.first(ColNames.start_timestamp).alias(ColNames.trip_start_timestamp),
            F.collect_list(F.col(ColNames.time_segment_id)).alias(
                ColNames.time_segment_ids_list
            ),  # NOTE: ordering is not guaranteed
            F.lit(self.current_month_date_min.year).alias(ColNames.year),
            F.lit(self.current_month_date_min.month).alias(ColNames.month),
            F.first(ColNames.user_id_modulo).alias(ColNames.user_id_modulo),
            F.lit(ReservedDatasetIDs.ABROAD).alias(ColNames.dataset_id),
        )

        # Omit trips that start in the lookahead window.
        trips_df = trips_df.filter(F.col(ColNames.trip_start_timestamp) <= self.current_month_date_max)
        trips_df = apply_schema_casting(trips_df, SilverTourismTripDataObject.SCHEMA)
        trips_df = trips_df.repartition(*SilverTourismTripDataObject.PARTITION_COLUMNS)

        self.output_data_objects[SilverTourismTripDataObject.ID].df = trips_df

        relevant_stays_df = self.calculate_visits(relevant_stays_df)

        aggregations_df = relevant_stays_df.groupBy(ColNames.iso2).agg(
            # Sum of overnight stays in countries for the current month
            F.sum(F.when((F.col("is_visit_finished")) & (F.col(ColNames.is_overnight)), 1).otherwise(0)).alias(
                ColNames.nights_spent
            ),
        )

        aggregations_df = (
            aggregations_df.withColumn(ColNames.time_period, F.lit(self.current_month_time_period_string))
            .withColumn(ColNames.year, F.lit(self.current_month_date_min.year))
            .withColumn(ColNames.month, F.lit(self.current_month_date_min.month))
            .withColumnRenamed(ColNames.iso2, ColNames.country_of_destination)
        )

        aggregations_df = apply_schema_casting(aggregations_df, SilverTourismOutboundNightsSpentDataObject.SCHEMA)
        aggregations_df = aggregations_df.repartition(*SilverTourismOutboundNightsSpentDataObject.PARTITION_COLUMNS)
        self.output_data_objects[SilverTourismOutboundNightsSpentDataObject.ID].df = aggregations_df

    def mark_overnight_segments(self, stays_df):
        """Marks segments as overnight or not based on local time zone.

        Args:
            stays_df (pyspark.sql.DataFrame): DataFrame containing stay records

        Returns:
            pyspark.sql.DataFrame: DataFrame with `is_overnight` column added.
        """

        # Calculate local start and end timestamps
        stays_df = (
            stays_df.withColumn(
                "local_start_timestamp",
                F.from_utc_timestamp(F.col(ColNames.start_timestamp), F.col(ColNames.timezone)),
            )
            .withColumn(
                "local_end_timestamp",
                F.from_utc_timestamp(F.col(ColNames.end_timestamp), F.col(ColNames.timezone)),
            )
            .withColumn(  # Midnight timestamp step 1: midnight hour of first date
                "first_midnight",
                F.make_timestamp(
                    F.year(F.col("local_start_timestamp")),
                    F.month(F.col("local_start_timestamp")),
                    F.dayofmonth(F.col("local_start_timestamp")),
                    F.lit(self.functional_midnight_h),
                    F.lit(0),
                    F.lit(0),
                ),
            )
            .withColumn(  # Midnight timestamp step 2: shift first midnight timestamp to the next day if it is before the start timestamp
                "first_midnight",
                F.when(
                    F.col("first_midnight") < F.col("local_start_timestamp"),
                    F.col("first_midnight") + timedelta(days=1),
                ).otherwise(F.col("first_midnight")),
            )
        )

        # Mark segments that contain the functional midnight hour and are sufficiently long as overnight segments.
        stays_df = stays_df.withColumn(
            ColNames.is_overnight,
            F.when(
                (
                    (F.col("local_start_timestamp") < F.col("first_midnight"))
                    & (F.col("local_end_timestamp") >= F.col("first_midnight"))
                    &
                    # Duration check
                    (
                        (F.col("local_end_timestamp").cast("long") - F.col("local_start_timestamp").cast("long")) / 60.0
                        >= self.min_duration_segment_night_m
                    )
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        ).drop("local_start_timestamp", "local_end_timestamp", "first_midnight")

        return stays_df

    def calculate_trips(self, stays_df):
        """Calculates trip information for segments.

        This function processes stay records to identify and mark distinct trips based on temporal gaps
        between consecutive stays for each user. It assigns trip IDs, determines trip start timestamps,
        and marks whether trips are finished in the current month.

        Args:
            stays_df (pyspark.sql.DataFrame): DataFrame containing stay records

        Returns:
            pyspark.sql.DataFrame: DataFrame with trip information added.

        Notes:
            - A new trip is created when the time gap between consecutive stays exceeds max_trip_gap_h
            - Existing trip_ids are preserved if present in the input DataFrame
            - Trip completion is determined based on the current_month_date_min
        """

        # Define window for sorting stays within each user
        user_window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id).orderBy(ColNames.start_timestamp)

        # Identify new trips based on the maximum allowed time gap between stays
        # and if there is no previous ongoing trip
        stays_df = stays_df.withColumn(
            "prev_end_timestamp", F.lag(ColNames.end_timestamp).over(user_window)
        ).withColumn(
            "is_new_trip",
            F.when((F.col("prev_end_timestamp").isNull()) & (F.col(ColNames.trip_id).isNull()), F.lit(True))
            .when((F.col(ColNames.trip_id).isNotNull()), F.lit(False))
            .otherwise(
                (
                    (F.col(ColNames.start_timestamp).cast(LongType()) - F.col("prev_end_timestamp").cast(LongType()))
                    / 3600
                    > self.max_trip_gap_h
                )
            ),
        )

        # assign start timestamp of the trip
        stays_df = stays_df.withColumn(
            ColNames.trip_start_timestamp,
            F.when(
                F.col("is_new_trip"),
                F.col(ColNames.start_timestamp),
            ).otherwise(F.col(ColNames.trip_start_timestamp)),
        ).withColumn(  # forward fill trip start timestamp
            ColNames.trip_start_timestamp,
            F.last(ColNames.trip_start_timestamp, ignorenulls=True).over(user_window),
        )
        # Generate trip ID as a hash of user ID and trip start time, but reuse existing trip IDs if available
        stays_df = stays_df.withColumn(
            ColNames.trip_id,
            F.when(
                F.col(ColNames.trip_id).isNull(),  # Generate new trip_id only if it does not already exist
                F.md5(
                    F.concat(
                        F.col(ColNames.user_id).cast(StringType()),
                        F.col(ColNames.trip_start_timestamp).cast(StringType()),
                    )
                ),
            ).otherwise(F.col(ColNames.trip_id)),
        ).drop("prev_end_timestamp")

        # Determine if a trip is finished based on the maximum `end_timestamp` in each trip
        trip_window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.trip_id)
        stays_df = stays_df.withColumn(
            "is_trip_finished",
            F.when(
                F.max(ColNames.end_timestamp).over(trip_window).cast("date") <= F.lit(self.current_month_date_max),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return stays_df

    def calculate_visits(self, stays_df):
        """Calculates visit IDs for stays data and determines if visits are finished in the current month.

        A new visit is created when:
        1. It's the first record in the trip
        2. There's a change in the country visited

        Args:
            stays_df (pyspark.sql.DataFrame): DataFrame containing stay records

        Returns:
            pyspark.sql.DataFrame: DataFrame with visit information added.

        """

        # Define window for trip level sorting
        trip_window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.trip_id).orderBy(
            ColNames.start_timestamp
        )

        stays_df = stays_df.withColumn(
            "prev_end_timestamp", F.lag(ColNames.end_timestamp).over(trip_window)
        ).withColumn("prev_iso2", F.lag(ColNames.iso2).over(trip_window))

        # Flag new visits based on country change
        stays_df = stays_df.withColumn(
            ColNames.visit_id,
            F.sum(
                F.when(
                    (F.col("prev_end_timestamp").isNull())  # First record in the trip
                    | (F.col(ColNames.iso2) != F.col("prev_iso2")),  # New country visited
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).over(trip_window),
        ).drop("prev_end_timestamp", "prev_iso2")

        # Define a window for visit level sorting
        visit_window = Window.partitionBy(
            ColNames.user_id_modulo, ColNames.user_id, ColNames.trip_id, ColNames.visit_id
        )

        # Determine if a visit is finished in the current month
        stays_df = stays_df.withColumn(
            "is_visit_finished",
            F.when(
                F.month(F.max(ColNames.end_timestamp).over(visit_window).cast("date"))
                == F.lit(self.current_month_date_max.month),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return stays_df

    @staticmethod
    def get_period_month_bounds_list(
        data_period_start: datetime, data_period_end: datetime
    ) -> list[tuple[datetime.date, datetime.date]]:
        """Generate the first and last date of each month that is within the calculation period."""
        return_list = []
        current_month_start = data_period_start
        while current_month_start <= data_period_end:
            current_month_end = current_month_start + relativedelta(months=1) - timedelta(seconds=1)
            return_list.append((current_month_start.date(), current_month_end.date()))
            current_month_start += relativedelta(months=1)
        return return_list
