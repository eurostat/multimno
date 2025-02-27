"""
Module that implements the Tourism Stays Estimation functionality.
"""

from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from math import ceil

from multimno.core.data_objects.bronze.bronze_mcc_iso_tz_map import BronzeMccIsoTzMap
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject
from multimno.core.data_objects.silver.silver_tourism_stays_data_object import SilverTourismStaysDataObject

from multimno.core.component import Component
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, CONFIG_BRONZE_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting

from pyspark.sql.types import StringType, LongType


class TourismStatisticsCalculation(Component):
    """
    A class to calculate tourism statistics of geozones per time period.
    """

    COMPONENT_ID = "TourismStatisticsCalculation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.strptime(self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m")
        self.data_period_end = datetime.strptime(self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m")
        # Generate (month_first_date, month_last_date) pairs
        self.data_period_bounds_list = self.get_period_month_bounds_list(self.data_period_start, self.data_period_end)

        self.zoning_dataset_ids_and_levels_list = self.config.geteval(
            self.COMPONENT_ID, "zoning_dataset_ids_and_levels_list"
        )
        self.max_trip_gap_h = self.config.getint(self.COMPONENT_ID, "max_trip_gap_h")
        self.max_visit_gap_h = self.config.getint(self.COMPONENT_ID, "max_visit_gap_h")

    def initalize_data_objects(self):
        # Handle optional deletion of existing trips
        self.output_tourism_trip_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_trips_silver")
        self.delete_existing_trips = self.config.getboolean(self.COMPONENT_ID, "delete_existing_trips")
        if self.delete_existing_trips:
            delete_file_or_folder(self.spark, self.output_tourism_trip_path)

        # Input
        self.input_data_objects = {}

        inputs = {
            "tourism_stays_silver": SilverTourismStaysDataObject,
            "mcc_iso_timezones_data_bronze": BronzeMccIsoTzMap,
            # "tourism_trips_silver": SilverTourismTripDataObject
        }

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
        # Also write empty dataframe to path (workaround for read() having no target path).
        path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_trips_silver")
        self.input_data_objects[SilverTourismTripDataObject.ID] = SilverTourismTripDataObject(self.spark, path)
        if not check_if_data_path_exists(self.spark, path):
            self.input_data_objects[SilverTourismTripDataObject.ID].df = self.spark.createDataFrame(
                [], schema=SilverTourismTripDataObject.SCHEMA
            )
            self.input_data_objects[SilverTourismTripDataObject.ID].write()

        # Output
        self.output_data_objects = {}

        self.output_data_objects[SilverTourismTripDataObject.ID] = SilverTourismTripDataObject(
            self.spark,
            self.output_tourism_trip_path,
        )

        self.output_tourism_geozone_aggregations_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "tourism_geozone_aggregations_silver"
        )
        self.output_data_objects[SilverTourismZoneDeparturesNightsSpentDataObject.ID] = (
            SilverTourismZoneDeparturesNightsSpentDataObject(
                self.spark,
                self.output_tourism_geozone_aggregations_path,
            )
        )

        self.output_tourism_trip_aggregations_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "tourism_trip_aggregations_silver"
        )
        self.output_data_objects[SilverTourismTripAvgDestinationsNightsSpentDataObject.ID] = (
            SilverTourismTripAvgDestinationsNightsSpentDataObject(
                self.spark,
                self.output_tourism_trip_aggregations_path,
            )
        )

        # Output clearing
        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        if clear_destination_directory:
            delete_file_or_folder(self.spark, self.output_tourism_geozone_aggregations_path)
            delete_file_or_folder(self.spark, self.output_tourism_trip_aggregations_path)

    def write(self, data_object_id: str):
        self.logger.info(f"Writing {data_object_id} output...")
        self.output_data_objects[data_object_id].write()

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        # for every month within the data period, process stays and calculate aggregations.
        for dataset_id_and_hierarchical_levels_pair in self.zoning_dataset_ids_and_levels_list:
            self.current_zoning_dataset_id = dataset_id_and_hierarchical_levels_pair[0]
            self.hierarchical_levels_to_calculate = dataset_id_and_hierarchical_levels_pair[1]
            for current_month_date_min, current_month_date_max in self.data_period_bounds_list:
                self.read()

                # Handle mcc to iso timezone mapping data.
                self.mcc_iso_tz_map_df = self.input_data_objects[BronzeMccIsoTzMap.ID].df.select(
                    F.col(ColNames.mcc).alias("mcc_map"), ColNames.iso2
                )

                self.current_month_date_min, self.current_month_date_max = (
                    current_month_date_min,
                    current_month_date_max,
                )
                self.current_month_time_period_string = (
                    f"{current_month_date_min.year}-{current_month_date_min.month:0>2}"
                )
                # Increase date_max to include stays from the next month within max_trip_gap_h
                input_date_max = current_month_date_max + timedelta(days=ceil(self.max_trip_gap_h / 24.0))
                input_timestamp_max = input_date_max + timedelta(hours=self.max_trip_gap_h)
                self.logger.info(
                    f"Processing stays in date range {current_month_date_min.strftime('%Y-%m-%d')} - {current_month_date_max.strftime('%Y-%m-%d')}, look-forward timestamp max: {input_timestamp_max}"
                )

                # Select stays of the current month plus those within the look-forward window
                current_month_stays_df = (
                    self.input_data_objects[SilverTourismStaysDataObject.ID]
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
                        & (F.col(ColNames.dataset_id) == self.current_zoning_dataset_id)
                    )
                    .select(
                        F.col(ColNames.user_id),
                        F.col(ColNames.time_segment_id),
                        F.lit(None).alias(ColNames.trip_id),
                        F.lit(None).alias(ColNames.trip_start_timestamp),
                        F.col(ColNames.zone_ids_list),
                        F.col(ColNames.zone_weights_list),
                        F.col(ColNames.start_timestamp),
                        F.col(ColNames.end_timestamp),
                        F.col(ColNames.is_overnight),
                        F.col(ColNames.user_id_modulo),
                        F.col(ColNames.mcc),
                        F.col(ColNames.year),
                        F.col(ColNames.month),
                    )
                )

                # Get previous ongoing trips. Select trips that were unfinished in the previous month.
                ongoing_trips_df = self.input_data_objects[SilverTourismTripDataObject.ID].df.filter(
                    (F.col(ColNames.is_trip_finished) == False)
                    & (F.col(ColNames.year) == (current_month_date_min - relativedelta(months=1)).year)
                    & (F.col(ColNames.month) == (current_month_date_min - relativedelta(months=1)).month)
                    & (F.col(ColNames.dataset_id) == self.current_zoning_dataset_id)
                )
                if ongoing_trips_df.count() == 0:
                    self.relevant_stays_df = current_month_stays_df
                    self.logger.info("No ongoing trips from the previous month.")
                else:
                    # get earliest start timestamp of the trip
                    earliest_start_timestamp = ongoing_trips_df.select(F.min(ColNames.trip_start_timestamp)).collect()[
                        0
                    ][0]

                    # get all stays starting from earliest date of ongoing trips
                    last_month_stays_df = (
                        self.input_data_objects[SilverTourismStaysDataObject.ID]
                        .df.filter(
                            (F.col(ColNames.start_timestamp) >= earliest_start_timestamp)
                            & (F.col(ColNames.end_timestamp) < current_month_date_min)
                        )
                        .select(
                            F.col(ColNames.user_id),
                            F.col(ColNames.time_segment_id),
                            F.col(ColNames.zone_ids_list),
                            F.col(ColNames.zone_weights_list),
                            F.col(ColNames.start_timestamp),
                            F.col(ColNames.end_timestamp),
                            F.col(ColNames.is_overnight),
                            F.col(ColNames.user_id_modulo),
                            F.col(ColNames.mcc),
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
                self.spark.catalog.clearCache()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        relevant_stays_df = self.relevant_stays_df
        mcc_iso_tz_map_df = self.mcc_iso_tz_map_df

        # Calculate trips
        relevant_stays_df = self.calculate_trips(relevant_stays_df)
        relevant_stays_df = relevant_stays_df.cache()

        # group stays to trips collect list of segment ids.
        trips_df = relevant_stays_df.groupBy(ColNames.user_id, ColNames.trip_id).agg(
            F.first(ColNames.is_trip_finished).alias(ColNames.is_trip_finished),
            F.first(ColNames.start_timestamp).alias(ColNames.trip_start_timestamp),
            F.collect_list(F.col(ColNames.time_segment_id)).alias(
                ColNames.time_segment_ids_list
            ),  # NOTE: order is not deterministic
            F.lit(self.current_month_date_min.year).alias(ColNames.year),
            F.lit(self.current_month_date_min.month).alias(ColNames.month),
            F.min(ColNames.user_id_modulo).alias(ColNames.user_id_modulo),
            F.lit(self.current_zoning_dataset_id).alias(ColNames.dataset_id),
        )

        trips_df = apply_schema_casting(trips_df, SilverTourismTripDataObject.SCHEMA)
        trips_df = trips_df.repartition(*SilverTourismTripDataObject.PARTITION_COLUMNS)
        # NOTE: This will be written as output at the end. It is not written right away to avoid doubling rows in the stays dataframe due to cache() behaviour.

        # Join stays with mcc_iso_tz_map to get iso2 for each mcc
        relevant_stays_df = relevant_stays_df.join(
            mcc_iso_tz_map_df,
            F.col(ColNames.mcc) == F.col("mcc_map"),
            "left",
        ).drop("mcc_map")

        relevant_stays_df = relevant_stays_df.withColumn(
            ColNames.country_of_origin, F.coalesce(F.col(ColNames.iso2), F.lit("XX"))
        ).drop("iso2", ColNames.mcc)

        # Explode zone arrays and weights
        relevant_stays_df = (
            relevant_stays_df.withColumn(
                "zipped_arrays",
                F.explode(F.arrays_zip(F.col(ColNames.zone_ids_list), F.col(ColNames.zone_weights_list))),
            )
            .withColumn(ColNames.hierarchical_id, F.col(f"zipped_arrays.{ColNames.zone_ids_list}"))
            .withColumn(ColNames.zone_weight, F.col(f"zipped_arrays.{ColNames.zone_weights_list}"))
            .drop("zipped_arrays", ColNames.zone_ids_list, ColNames.zone_weights_list)
        )

        for hierarchical_level in self.hierarchical_levels_to_calculate:
            self.logger.info(f"Calculating aggregations for hierarchical level {hierarchical_level}...")

            # get current level zone ids
            hierarchical_level_stays_df = relevant_stays_df.withColumn(
                ColNames.zone_id,
                F.element_at(F.split(F.col(ColNames.hierarchical_id), pattern="\\|"), hierarchical_level),
            )

            # calculate visits to current level zones
            hierarchical_level_stays_df = self.calculate_visits(hierarchical_level_stays_df)

            # Perform aggregations I (nights spent and departures from geographical unit)
            # I.1 Calculate nights spent per zone, applying zone weights per stay.
            # I.2 Calculate departures per zone for overnight and non-overnight visits
            # TODO: Potentially calculate departures based on weights of zones visited
            aggregations_i_df = hierarchical_level_stays_df.groupBy(
                ColNames.country_of_origin, ColNames.is_overnight, ColNames.zone_id
            ).agg(
                # Sum of zone_weights for finished visits in the current month
                F.sum(
                    F.when(
                        (F.col("is_visit_finished")) & (F.col(ColNames.is_overnight)), F.col(ColNames.zone_weight)
                    ).otherwise(0)
                ).alias(ColNames.nights_spent),
                # Count of unique zone_ids for finished trips
                F.countDistinct(F.when(F.col(ColNames.is_trip_finished), F.col(ColNames.trip_id))).alias(
                    ColNames.num_of_departures
                ),
            )

            aggregations_i_df = (
                aggregations_i_df.withColumn(ColNames.time_period, F.lit(self.current_month_time_period_string))
                .withColumn(ColNames.level, F.lit(hierarchical_level))
                .withColumn(ColNames.year, F.lit(self.current_month_date_min.year))
                .withColumn(ColNames.month, F.lit(self.current_month_date_min.month))
                .withColumn(ColNames.dataset_id, F.lit(self.current_zoning_dataset_id))
            )

            aggregations_i_df = apply_schema_casting(
                aggregations_i_df, SilverTourismZoneDeparturesNightsSpentDataObject.SCHEMA
            )

            aggregations_i_df = aggregations_i_df.repartition(
                *SilverTourismZoneDeparturesNightsSpentDataObject.PARTITION_COLUMNS
            )

            # Write output. Append to existing if previous hierarchical level exists.
            self.output_data_objects[SilverTourismZoneDeparturesNightsSpentDataObject.ID].df = aggregations_i_df

            # Perform aggregations II
            # These apply only to trips completed in the current month.
            current_month_relevant_stays_df = hierarchical_level_stays_df.where(F.col(ColNames.is_trip_finished))
            # II.1 Average number of destinations per trip completed this month.
            avg_destination_cnt_df = self.calculate_avg_destination_cnt(current_month_relevant_stays_df)

            # II.2 Average number of nights spent per destination.
            avg_number_of_nights_spent_per_destination_df = self.calculate_avg_nights_spent_per_destination(
                current_month_relevant_stays_df
            )

            # Combine average destination count per trip and average number of nights spent per destination.
            aggregations_ii_df = (
                avg_destination_cnt_df.alias("df1")
                .join(
                    avg_number_of_nights_spent_per_destination_df.alias("df2"),
                    on=F.col(f"df1.{ColNames.country_of_origin}") == F.col(f"df2.{ColNames.country_of_origin}"),
                )
                .select(
                    F.lit(self.current_month_time_period_string).alias(ColNames.time_period),
                    F.col(f"df1.{ColNames.country_of_origin}"),
                    F.col(ColNames.avg_destinations),
                    F.col(ColNames.avg_nights_spent_per_destination),
                    F.lit(self.current_month_date_min.year).alias(ColNames.year),
                    F.lit(self.current_month_date_min.month).alias(ColNames.month),
                    F.lit(hierarchical_level).alias(ColNames.level),
                    F.lit(self.current_zoning_dataset_id).alias(ColNames.dataset_id),
                )
            )

            aggregations_ii_df = apply_schema_casting(
                aggregations_ii_df, SilverTourismTripAvgDestinationsNightsSpentDataObject.SCHEMA
            )

            aggregations_ii_df = aggregations_ii_df.repartition(
                *SilverTourismTripAvgDestinationsNightsSpentDataObject.PARTITION_COLUMNS
            )

            # Write output. Append to existing if previous hierarchical level exists.
            self.output_data_objects[SilverTourismTripAvgDestinationsNightsSpentDataObject.ID].df = aggregations_ii_df

            self.write(SilverTourismZoneDeparturesNightsSpentDataObject.ID)
            self.write(SilverTourismTripAvgDestinationsNightsSpentDataObject.ID)

        # Write current month trips to output
        # Careful when you execute this, since it messes with cached stays.
        self.output_data_objects[SilverTourismTripDataObject.ID].df = trips_df
        self.write(SilverTourismTripDataObject.ID)

    def calculate_trips(self, stays_df):
        """Calculates trip information for stays data using PySpark DataFrame operations.

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
        2. There's a change in the visited zone
        3. Time gap between consecutive stays exceeds max_visit_gap_h

        Args:
            stays_df (pyspark.sql.DataFrame): DataFrame containing stay records

        Returns:
            pyspark.sql.DataFrame: DataFrame with visit information added.

        Notes:
            The function uses window functions to partition data by user and trip for proper
            visit calculation and utilizes self.max_visit_gap_h and self.current_month_date_min
            class attributes for gap threshold and current month validation respectively.
        """

        # Define window for trip level sorting
        # Sorting by zone_id first allows aggregating by zone regardless of stay sequentiality
        trip_window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.trip_id).orderBy(
            ColNames.zone_id, ColNames.start_timestamp
        )

        stays_df = stays_df.withColumn(
            "prev_end_timestamp", F.lag(ColNames.end_timestamp).over(trip_window)
        ).withColumn("prev_zone_id", F.lag(ColNames.zone_id).over(trip_window))

        # Flag new visits based on time gap or zone change
        stays_df = stays_df.withColumn(
            ColNames.visit_id,
            F.sum(
                F.when(
                    (F.col("prev_end_timestamp").isNull())  # First record in the trip
                    | (F.col(ColNames.zone_id) != F.col("prev_zone_id"))  # New zone visited
                    | (
                        (
                            F.col(ColNames.start_timestamp).cast(LongType())
                            - F.col("prev_end_timestamp").cast(LongType())
                        )
                        / 3600
                        > self.max_visit_gap_h  # Time gap exceeds threshold
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).over(trip_window),
        ).drop("prev_end_timestamp", "prev_zone_id")

        # Define a window for visit level sorting
        visit_window = Window.partitionBy(
            ColNames.user_id_modulo, ColNames.user_id, ColNames.trip_id, ColNames.visit_id
        )

        # Determine if a visit is finished in the current month
        stays_df = stays_df.withColumn(
            "is_visit_finished",
            F.when(
                F.month(F.max(ColNames.end_timestamp).over(visit_window)) == F.lit(self.current_month_date_max.month),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return stays_df

    def calculate_avg_nights_spent_per_destination(self, current_month_finished_trip_stays_df):
        """Calculates the average number of nights spent per destination grouped by MCC.

        This function processes trip stays data to compute the average number of overnight stays
        for each destination category (MCC). It first aggregates nights spent per user-trip-zone
        combination, then calculates the average across all users for each MCC.

        Args:
            current_month_finished_trip_stays_df (DataFrame): Spark DataFrame containing finished trip stays data

        Returns:
            DataFrame: A Spark DataFrame with columns for MCC and the average number of nights spent per destination.

        Note:
            The function uses zone_weight to account for partial stays when calculating nights spent.
            Only records where is_overnight is True are considered for the nights calculation.
        """

        avg_number_of_nights_spent_per_destination_df = (
            current_month_finished_trip_stays_df.groupBy(ColNames.user_id, ColNames.trip_id, ColNames.zone_id)
            .agg(
                F.sum(F.when(F.col(ColNames.is_overnight), F.col(ColNames.zone_weight)).otherwise(F.lit(0.0))).alias(
                    "nights_spent"
                ),
                F.first(F.col(ColNames.country_of_origin)).alias(ColNames.country_of_origin),
            )
            .groupBy(ColNames.country_of_origin)
            .agg(F.avg(F.col("nights_spent")).alias(ColNames.avg_nights_spent_per_destination))
        )

        return avg_number_of_nights_spent_per_destination_df

    def calculate_avg_destination_cnt(self, current_month_finished_trip_stays_df):
        """Calculate average number of unique destinations per trip grouped by MCC.

        Args:
            current_month_finished_trip_stays_df (DataFrame): Spark DataFrame containing trip stays data

        Returns:
            DataFrame: A Spark DataFrame with the average number of unique destinations per trip.
        """

        avg_destination_cnt_df = (
            current_month_finished_trip_stays_df.groupBy(ColNames.user_id, ColNames.trip_id)
            .agg(
                F.count_distinct(F.col(ColNames.zone_id)).alias("nr_of_unique_destinations"),
                F.first(F.col(ColNames.country_of_origin)).alias(ColNames.country_of_origin),
            )
            .groupBy(ColNames.country_of_origin)
            .agg(F.avg(F.col("nr_of_unique_destinations")).alias(ColNames.avg_destinations))
        )

        return avg_destination_cnt_df

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
