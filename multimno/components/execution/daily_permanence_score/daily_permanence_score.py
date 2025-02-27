"""
Module that implements the Daily Permanence Score functionality
"""

from typing import List, Tuple, Set

from datetime import datetime, timedelta, date
from multimno.core.data_objects.silver.event_cache_data_object import EventCacheDataObject
from multimno.core.spark_session import delete_file_or_folder
from pyspark.sql import DataFrame, Window
from pyspark import StorageLevel
import pyspark.sql.functions as F
from sedona.sql import st_functions as STF
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    FloatType,
    IntegerType,
    ShortType,
    ByteType,
    ArrayType,
    LongType,
    StringType,
)

from multimno.core.component import Component

from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, GENERAL_CONFIG_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import UeGridIdType
from multimno.core.grid import InspireGridGenerator
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


class DailyPermanenceScore(Component):
    """
    A class to calculate the daily permanence score of each user per interval and grid tile.
    """

    COMPONENT_ID = "DailyPermanenceScore"
    # Only 24, 48, and 96 are allowed as values for the number of time slots. These values correspond to intervals
    # of length 60, 30, and 15 minutes respectively.
    ALLOWED_NUMBER_OF_TIME_SLOTS = [24, 48, 96]

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.time_slot_number = self.config.getint(self.COMPONENT_ID, "time_slot_number")
        if self.time_slot_number not in self.ALLOWED_NUMBER_OF_TIME_SLOTS:
            raise ValueError(
                f"Accepted values for `time_slot_number` are {str(self.ALLOWED_NUMBER_OF_TIME_SLOTS)} -- found {self.time_slot_number}"
            )

        self.max_time_thresh = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_thresh"))
        self.max_time_thresh_day = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_thresh_day"))
        self.max_time_thresh_night = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_thresh_night"))
        self.max_time_thresh_abroad = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_thresh_abroad"))
        self.max_speed_thresh = self.config.getfloat(self.COMPONENT_ID, "max_speed_thresh")
        self.broadcast_footprints = self.config.getboolean(self.COMPONENT_ID, "broadcast_footprints", fallback=False)
        self.use_200m_grid = self.config.getboolean(self.COMPONENT_ID, "use_200m_grid", fallback=False)
        self.local_mcc = self.config.getint(GENERAL_CONFIG_KEY, "local_mcc")
        self.event_error_flags_to_include = self.config.geteval(self.COMPONENT_ID, "event_error_flags_to_include")

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        self.grid_gen = InspireGridGenerator(self.spark)
        self.events = None
        self.cell_footprint = None
        self.time_slots = None
        self.current_date = None

    def initalize_data_objects(self):
        # Get paths
        input_events_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver_flagged")
        input_events_cache_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_cache")
        input_cell_footprint_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        output_dps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_data_silver")

        # Clear destination directory if needed
        clear_destination_directory = self.config.getboolean(
            self.COMPONENT_ID, "clear_destination_directory", fallback=False
        )
        if clear_destination_directory:
            self.logger.warning(f"Deleting: {output_dps_path}")
            delete_file_or_folder(self.spark, output_dps_path)

        # ------------------ Data objects ------------------
        silver_events = SilverEventFlaggedDataObject(self.spark, input_events_silver_path)
        silver_cell_footprint = SilverCellFootprintDataObject(self.spark, input_cell_footprint_silver_path)
        events_cache = EventCacheDataObject(self.spark, input_events_cache_path)

        silver_dps = SilverDailyPermanenceScoreDataObject(self.spark, output_dps_path)

        self.input_data_objects = {
            silver_events.ID: silver_events,
            silver_cell_footprint.ID: silver_cell_footprint,
            events_cache.ID: events_cache,
        }

        self.output_data_objects = {silver_dps.ID: silver_dps}

    # ------------------ Assert existence of input data and read+filter it. ------------------

    def check_needed_dates(self):
        """
        Method that checks if both the dates of study and the dates necessary to generate
        the daily permanence scores are present in the input data (events + cell footprint).

        Raises:
            ValueError: If there is no data for one or more of the needed dates.
        """
        # needed dates: for each date D, we also need D-1 and D+1
        # this is built this way so it would also support definition of study
        # dates that are not consecutive
        needed_dates = (
            {d + timedelta(days=1) for d in self.data_period_dates}
            | set(self.data_period_dates)
            | {d - timedelta(days=1) for d in self.data_period_dates}
        )
        self.logger.info(needed_dates)
        # Assert needed dates in event data:
        self.assert_needed_dates_events()

        # Assert needed dates in cell footprint data:
        self.assert_needed_dates_data_object(SilverCellFootprintDataObject.ID, needed_dates)

    def assert_needed_dates_events(self):
        extended_dates = {d + timedelta(days=1) for d in self.data_period_dates} | {
            d - timedelta(days=1) for d in self.data_period_dates
        }

        for dates_to_check, event_type_do in zip(
            [self.data_period_dates, extended_dates], [SilverEventFlaggedDataObject, EventCacheDataObject]
        ):
            # Check event data for data_period_dates
            missing_dates = [
                date for date in dates_to_check if not self.input_data_objects[event_type_do.ID].is_data_available(date)
            ]
            # Report missing dates
            if missing_dates:
                error_msg = f"Missing {event_type_do.ID} data for dates {sorted(list(missing_dates))}"
                self.logger.error(error_msg)
                raise ValueError(error_msg)

        # Check event cache data for extended_dates
        missing_dates = [
            date
            for date in extended_dates
            if not self.input_data_objects[EventCacheDataObject.ID].is_data_available(date)
        ]

    def assert_needed_dates_data_object(self, data_object_id: str, needed_dates: List[datetime]):
        """
        Method that checks if data for a set of dates exists for a data object.

        Args:
            data_object_id (str): name of the data object to check.
            needed_dates (List[datetime]): list of the dates for which data shall be available.

        Raises:
            ValueError: If there is no data for one or more of the needed dates.
        """
        # Load data
        df = self.input_data_objects[data_object_id].df

        # Find dates that match the needed dates:
        dates = (
            df.withColumn(ColNames.date, F.make_date(ColNames.year, ColNames.month, ColNames.day))
            .select(F.col(ColNames.date))
            .filter(F.col(ColNames.date).isin(needed_dates))
            .distinct()
            .collect()
        )
        available_dates = {row[ColNames.date] for row in dates}

        # If missing needed dates, raise error:
        missing_dates = needed_dates.difference(available_dates)
        if missing_dates:
            error_msg = f"Missing {data_object_id} data for dates {sorted(list(missing_dates))}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def filter_events(self, current_date: date, partition_chunk=None) -> DataFrame:
        """
        Load events with no errors for a specific date.

        Args:
            current_date (date): current date.

        Returns:
            DataFrame: filtered events dataframe.
        """
        df = (
            self.input_data_objects[SilverEventFlaggedDataObject.ID]
            .df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
                & (F.col(ColNames.error_flag).isin(self.event_error_flags_to_include))
            )
            .select(
                ColNames.user_id,
                ColNames.cell_id,
                ColNames.timestamp,
                ColNames.plmn,
                ColNames.year,
                ColNames.month,
                ColNames.day,
                ColNames.user_id_modulo,
            )
        )

        if partition_chunk is not None:
            df = df.filter(F.col(ColNames.user_id_modulo).isin(partition_chunk))

        return df

    def get_cache_events(self, current_date: date, partition_chunk=None, last_event: bool = True) -> DataFrame:
        """
        Load cache events with for a specific date.

        Args:
            current_date (date): current date.
            last_event (bool): flag to get last event or first.

        Returns:
            DataFrame: filtered events dataframe.
        """
        df = (
            self.input_data_objects[EventCacheDataObject.ID]
            .df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            )
            .filter(F.col(ColNames.is_last_event) == last_event)
            .drop(ColNames.is_last_event)
        ).select(
            ColNames.user_id,
            ColNames.cell_id,
            ColNames.timestamp,
            ColNames.plmn,
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id_modulo,
        )

        if partition_chunk is not None:
            df = df.filter(F.col(ColNames.user_id_modulo).isin(partition_chunk))

        return df

    def filter_cell_footprint(self, current_date: date, cells: DataFrame = None) -> DataFrame:
        """
        Load cell footprints for a specific date.

        Args:
            current_date (date): current date.

        Returns:
            DataFrame: filtered cell footprint dataframe.
        """
        df = self.input_data_objects[SilverCellFootprintDataObject.ID].df.filter(
            (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
        )

        if cells is not None:
            df = df.join(cells, ColNames.cell_id, "inner")

        return df

    # ------------------ Execute ------------------
    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        self.check_needed_dates()
        partition_chunks = self._get_partition_chunks()

        for current_date in self.data_period_dates:
            self.current_date = current_date  # for use in other methods
            self.logger.info(f"Processing events for {current_date.strftime('%Y-%m-%d')}")
            self.build_time_slots_table(current_date)

            for i, partition_chunk in enumerate(partition_chunks):
                self.logger.info(f"Processing partition chunk {i}")
                self.logger.debug(f"Partition chunk: {partition_chunk}")

                # Build input data
                self.build_day_data(current_date, partition_chunk)

                # Transform
                self.transform()

                # Write
                self.write()
                self.spark.catalog.clearCache()
                self.logger.info(f"Finished partition chunk {i}")

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def _get_partition_chunks(self) -> List[List[int]]:
        """
        Method that returns the partition chunks for the current date.

        Returns:
            List[List[int, int]]: list of partition chunks. If the partition_chunk_size is not defined in the config or
                the number of partitions is less than the desired chunk size, it will return a list with a single None element.
        """
        # Get partitions desired
        partition_chunk_size = self.config.getint(self.COMPONENT_ID, "partition_chunk_size", fallback=None)
        number_of_partitions = self.config.getint(self.COMPONENT_ID, "number_of_partitions", fallback=None)

        if partition_chunk_size is None or number_of_partitions is None or partition_chunk_size <= 0:
            return [None]

        if number_of_partitions <= partition_chunk_size:
            self.logger.warning(
                f"Available Partition number ({number_of_partitions}) is "
                f"less than the desired chunk size ({partition_chunk_size}). "
                f"Using all partitions."
            )
            return [None]
        partition_chunks = [
            list(range(i, min(i + partition_chunk_size, number_of_partitions)))
            for i in range(0, number_of_partitions, partition_chunk_size)
        ]
        # NOTE: Generate chunks if partition_values were read for each day
        # getting exactly the amount of partitions for that day

        # partition_chunks = [
        #     partition_values[i : i + partition_chunk_size]
        #     for i in range(0, partition_values_size, partition_chunk_size)
        # ]

        return partition_chunks

    def build_day_data(self, current_date, partition_chunk=None):
        """
        Load events data for date D, also adding last event of each
        user from date D-1 and first event of each user from D+1.

        Args:
            current_date (date): current date.

        Returns:
            DataFrame: events dataframe.
        """
        previous_date = current_date - timedelta(days=1)
        next_date = current_date + timedelta(days=1)

        current_events = self.filter_events(current_date, partition_chunk)
        previous_events = self.get_cache_events(previous_date, partition_chunk, last_event=True)
        next_events = self.get_cache_events(next_date, partition_chunk, last_event=False)

        # concat all events together (last of D-1 + all D + first of D+1 dummy outbound):
        events = previous_events.union(current_events).union(next_events)

        # Get distinct cell_ids for previous and next events for filtering footprints
        previous_cells = previous_events.select(ColNames.cell_id).distinct()
        next_cells = next_events.select(ColNames.cell_id).distinct()

        current_cell_footprint = self.filter_cell_footprint(current_date)
        previous_cell_footprint = self.filter_cell_footprint(previous_date, previous_cells)
        next_cell_footprint = self.filter_cell_footprint(next_date, next_cells)

        cell_footprint = previous_cell_footprint.union(current_cell_footprint).union(next_cell_footprint)

        cell_footprint = self.grid_gen.grid_ids_to_centroids(cell_footprint)

        if self.use_200m_grid:
            self.logger.info("Using 200m grid for DPS calculation")
            cell_footprint = self.grid_gen.get_parent_grid_ids(cell_footprint, 200, parent_col_name=ColNames.grid_id)

        cell_footprint = cell_footprint.groupBy([ColNames.cell_id, ColNames.year, ColNames.month, ColNames.day]).agg(
            F.collect_list(ColNames.geometry).alias(ColNames.geometry),
            F.array_sort(F.collect_set(ColNames.grid_id)).alias("grid_ids"),
        )

        cell_footprint = cell_footprint.withColumn(
            ColNames.geometry,
            STF.ST_ConcaveHull(STF.ST_Collect(F.col(ColNames.geometry)), 0.8),
        )

        # Load class attributes
        self.events = events

        # TODO: This is questionable, use with care
        if self.broadcast_footprints:
            cell_footprint = F.broadcast(cell_footprint)

        self.cell_footprint = cell_footprint.persist(StorageLevel.MEMORY_AND_DISK)

    def build_time_slots_table(self, current_date) -> DataFrame:
        """
        Build a dataframe with the specified time slots for the current date.

        Returns:
            DataFrame: time slots dataframe.
        """
        time_slot_length = timedelta(days=1) / self.time_slot_number

        time_slots_list = []
        previous_end_time = datetime(
            year=current_date.year,
            month=current_date.month,
            day=current_date.day,
            hour=0,
            minute=0,
            second=0,
        )

        while previous_end_time.date() == current_date:
            init_time = previous_end_time
            end_time = init_time + time_slot_length
            time_slot = (init_time, end_time)
            time_slots_list.append(time_slot)
            previous_end_time = end_time

        schema = StructType(
            [
                StructField(ColNames.time_slot_initial_time, TimestampType(), True),
                StructField(ColNames.time_slot_end_time, TimestampType(), True),
            ]
        )

        self.time_slots = self.spark.createDataFrame(time_slots_list, schema=schema)

    # ------------------ Main transformations ------------------
    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        current_events = self.events

        current_events = self.enrich_events(current_events, self.cell_footprint)
        # differentiate 'move' events:
        current_events = self.detect_move_events(current_events)
        # Determine stay durations:
        stays = self.determine_stay_durations(current_events)
        # generate time slots per user
        # TODO: this might be an issue with big countries, probably will need to rethink
        unique_users = stays.select(ColNames.user_id, ColNames.user_id_modulo).distinct()
        unique_users = unique_users.repartition(ColNames.user_id_modulo)
        user_time_slots = unique_users.crossJoin(F.broadcast(self.time_slots))

        # Assign stay time slot, assign duration to time slots:
        stays_slots = self.calculate_time_slots_durations(stays, user_time_slots)

        # Filter out durations that would result in 0 DPS
        stays_slots = self.filter_zero_dps_durations(stays_slots)
        stays_slots = stays_slots.persist(StorageLevel.MEMORY_AND_DISK)
        stays_slots.count()  # for some reason better to force computation
        # collect grid_ids where DPS is 1
        dps = self.calculate_dps(stays_slots, self.cell_footprint)

        dps = (
            dps
            # since some stays may come from events in previous date, fix and always set current date:
            .withColumn(ColNames.year, F.lit(self.current_date.year).cast(ShortType()))
            .withColumn(ColNames.month, F.lit(self.current_date.month).cast(ByteType()))
            .withColumn(ColNames.day, F.lit(self.current_date.day).cast(ByteType()))
        )

        dps = apply_schema_casting(dps, SilverDailyPermanenceScoreDataObject.SCHEMA)

        dps = dps.repartition(*SilverDailyPermanenceScoreDataObject.PARTITION_COLUMNS)
        self.output_data_objects[SilverDailyPermanenceScoreDataObject.ID].df = dps

    def enrich_events(self, events: DataFrame, cell_footprint: DataFrame) -> DataFrame:
        """
        Enrich events with additional information

        Args:
            events (DataFrame): events dataframe.
            cell_footprint (DataFrame): cells footprint dataframe.

        Returns:
            DataFrame: enriched events dataframe.
        """

        events = self.join_footprints(events, cell_footprint, [ColNames.geometry])
        # add abroad flag
        events = (
            events.withColumn("abroad_mcc", F.substring(F.col(ColNames.plmn), 0, 3).cast(IntegerType()))
            .withColumn(
                "is_abroad",
                F.when(
                    (F.col(ColNames.plmn).isNotNull()) & (F.col("abroad_mcc") != self.local_mcc), F.lit(True)
                ).otherwise(F.lit(False)),
            )
            .withColumn(  # add abroad country mcc as cell_id for abroad events
                ColNames.cell_id,
                F.when(F.col("is_abroad") == True, F.col("abroad_mcc")).otherwise(F.col(ColNames.cell_id)),
            )
            .drop("abroad_mcc", ColNames.plmn)
        )
        # Add lags of timestamp, cell_id:
        window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id).orderBy(ColNames.timestamp)
        lag_fields = [ColNames.timestamp, ColNames.cell_id, ColNames.geometry]
        for lf in lag_fields:
            events = events.withColumn(f"{lf}_+1", F.lag(lf, -1).over(window)).withColumn(
                f"{lf}_-1", F.lag(lf, 1).over(window)
            )

        return events

    def detect_move_events(self, events: DataFrame) -> DataFrame:
        """
        Detect which of the events are associated to moves according to the
        distances/times from previous to posterior event and a speed threshold.

        Args:
            events (DataFrame): events dataframe.
            cell_footprint (DataFrame): cells footprint dataframe.

        Returns:
            DataFrame: events dataframe, with an additional 'is_move' boolean column.
        """

        window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id).orderBy(ColNames.timestamp)
        # Calculate distance between grid tiles associated to events -1, 0 and +1:
        # Calculate speeds and determine which rows are moves:
        events = (
            events.withColumn(
                "dist_0_+1",
                F.when(
                    F.col(ColNames.geometry).isNotNull() & F.col(f"{ColNames.geometry}_+1").isNotNull(),
                    STF.ST_Distance(F.col(ColNames.geometry), F.col(f"{ColNames.geometry}_+1")),
                ).otherwise(F.lit(0)),
            )
            # .withColumn(
            #     "dist_-1_0",
            #     STF.ST_Distance(F.col(f"{ColNames.geometry}_-1"), F.col(ColNames.geometry)),
            # )
            .withColumn(  # repeating the distance calculation is not necessary, a lagged column works:
                "dist_-1_0", F.lag("dist_0_+1", 1).over(window)
            )
            .withColumn(
                "dist_-1_+1",
                F.when(
                    F.col(f"{ColNames.geometry}_-1").isNotNull() & F.col(f"{ColNames.geometry}_+1").isNotNull(),
                    STF.ST_Distance(F.col(f"{ColNames.geometry}_-1"), F.col(f"{ColNames.geometry}_+1")),
                ).otherwise(F.lit(0)),
            )
            .withColumn(
                "time_difference",
                F.unix_timestamp(events[f"{ColNames.timestamp}_+1"])
                - F.unix_timestamp(events[f"{ColNames.timestamp}_-1"]),
            )
            .withColumn("max_dist", F.greatest(F.col("dist_-1_0") + F.col("dist_0_+1"), F.col("dist_-1_+1")))
            .withColumn("speed", F.col("max_dist") / F.col("time_difference"))
            .withColumn("is_move", F.when(F.col("speed") > self.max_speed_thresh, True).otherwise(False))
            .drop(
                "dist_0_+1",
                "dist_-1_0",
                "dist_-1_+1",
                f"{ColNames.geometry}_-1",
                f"{ColNames.geometry}_+1",
                ColNames.geometry,
                "time_difference",
                "max_dist",
                "speed",
            )
        )

        return events

    def determine_stay_durations(self, events: DataFrame) -> DataFrame:
        """
        Determine the start time and end time for each stay event.

        Args:
            events (DataFrame): events dataframe.

        Returns:
            DataFrame: stays dataframe (filtering out moves).
        """
        current_datetime = datetime(self.current_date.year, self.current_date.month, self.current_date.day)
        # TODO: night interval could be a config parameter instead of hardcoded!
        night_start_time = current_datetime - timedelta(hours=1)
        night_end_time = current_datetime + timedelta(hours=9)

        stays = (
            events
            # Filter out 'move' events (keep only stays), and also drop unneeded columns:
            .filter(F.col("is_move") == False)
            # Set applicable time thresholds:
            # if prev, next and current events:
            # - if the event is in the same cell as the previous one, and the previous event is within the night interval,
            # - set the threshold to max_time_thresh_night, otherwise set it to max_time_thresh_day
            # - if the event is not in the same cell as the previous one, set the threshold to max_time_thresh
            # if prev or next event is missing and current event is abroad:
            # - set missing timestamp to current timestamp +/- max_time_thresh_abroad
            .withColumn(
                "threshold_-1",
                F.when(
                    (F.col("cell_id") == F.col("cell_id_-1"))
                    & (F.col("timestamp") >= night_start_time)
                    & (F.col("timestamp_-1") <= night_end_time),
                    self.max_time_thresh_night,
                ).otherwise(
                    F.when(F.col("cell_id") == F.col("cell_id_-1"), self.max_time_thresh_day).otherwise(
                        self.max_time_thresh
                    )
                ),
            )
            .withColumn(
                "threshold_+1",
                F.when(
                    (F.col("cell_id") == F.col("cell_id_+1"))
                    & (F.col("timestamp") >= night_start_time)
                    & (F.col("timestamp_+1") <= night_end_time),
                    self.max_time_thresh_night,
                ).otherwise(
                    F.when(F.col("cell_id") == F.col("cell_id_+1"), self.max_time_thresh_day).otherwise(
                        self.max_time_thresh
                    )
                ),
            )
            # Calculate init_time and end_time according to thresholds and time differences between events:
            .withColumn(
                "init_time",
                F.when(
                    (F.col(f"{ColNames.timestamp}_-1").isNull()) & (F.col("is_abroad") == True),
                    F.col(ColNames.timestamp) - self.max_time_thresh_abroad,
                )
                .when(
                    F.col(ColNames.timestamp) - F.col(f"{ColNames.timestamp}_-1") <= F.col("threshold_-1"),
                    F.col(ColNames.timestamp) - (F.col(ColNames.timestamp) - F.col(f"{ColNames.timestamp}_-1")) / 2,
                )
                .otherwise(F.col(ColNames.timestamp) - self.max_time_thresh / 2),
            )
            .withColumn(
                "end_time",
                F.when(
                    (F.col(f"{ColNames.timestamp}_+1").isNull()) & (F.col("is_abroad") == True),
                    F.col(ColNames.timestamp) + self.max_time_thresh_abroad,
                )
                .when(
                    F.col(f"{ColNames.timestamp}_+1") - F.col(ColNames.timestamp) <= F.col("threshold_+1"),
                    F.col(f"{ColNames.timestamp}_+1")
                    - (F.col(f"{ColNames.timestamp}_+1") - F.col(ColNames.timestamp)) / 2,
                )
                .otherwise(F.col(ColNames.timestamp) + self.max_time_thresh / 2),
            )
            .drop(
                f"{ColNames.cell_id}_-1",
                f"{ColNames.cell_id}_+1",
                ColNames.timestamp,
                f"{ColNames.timestamp}_-1",
                f"{ColNames.timestamp}_+1",
                ColNames.mcc,
                "is_move",
                "threshold_-1",
                "threshold_+1",
            )
        )

        return stays

    def calculate_time_slots_durations(self, stays: DataFrame, user_time_slots: DataFrame) -> DataFrame:
        """Calculates duration of user stays within defined time slots.

        Joins stay records with time slot definitions and computes overlapping durations.
        For time slots with no stays, assigns a default duration of time_slot_number (1/24th day).
        Finally aggregates total stay duration per user, cell and time slot.

        Args:
            stays (DataFrame): User mobility stay records
            user_time_slots (DataFrame): User time slot definitions

        Returns:
            DataFrame: Aggregated stay durations per time slot
        """
        stays = (
            stays.join(
                user_time_slots.select(
                    F.col(ColNames.user_id).alias("time_slot_user_id"),
                    F.col(ColNames.user_id_modulo).alias("time_slot_user_id_modulo"),
                    ColNames.time_slot_end_time,
                    ColNames.time_slot_initial_time,
                ),
                (
                    (F.col("init_time") < F.col(ColNames.time_slot_end_time))
                    & (
                        F.col("end_time")
                        > F.col(
                            ColNames.time_slot_initial_time,
                        )
                    )
                    & (F.col(ColNames.user_id) == F.col("time_slot_user_id"))
                    & (F.col(ColNames.user_id_modulo) == F.col("time_slot_user_id_modulo"))
                ),
                how="right",
            )
            .withColumn("init_time", F.greatest(F.col("init_time"), F.col(ColNames.time_slot_initial_time)))
            .withColumn("end_time", F.least(F.col("end_time"), F.col(ColNames.time_slot_end_time)))
            .withColumn(
                ColNames.stay_duration,
                F.when(
                    F.col(ColNames.cell_id).isNotNull(),
                    F.unix_timestamp(F.col("end_time")) - F.unix_timestamp(F.col("init_time")),
                ).otherwise(F.lit((timedelta(days=1) / self.time_slot_number).total_seconds()).cast(IntegerType())),
            )
            .select(
                F.coalesce(ColNames.user_id, "time_slot_user_id").alias(ColNames.user_id),
                F.coalesce(ColNames.cell_id, F.lit(UeGridIdType.UNKNOWN)).alias(ColNames.cell_id),
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
                ColNames.stay_duration,
                F.coalesce(F.col("is_abroad"), F.lit(False)).alias("is_abroad"),
                ColNames.year,
                ColNames.month,
                ColNames.day,
                F.coalesce(ColNames.user_id_modulo, "time_slot_user_id_modulo").alias(ColNames.user_id_modulo),
            )
        )

        stays = stays.groupBy(
            ColNames.user_id,
            ColNames.cell_id,
            ColNames.time_slot_initial_time,
            ColNames.time_slot_end_time,
            "is_abroad",
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id_modulo,
        ).agg((F.sum(ColNames.stay_duration).cast(FloatType()).alias(ColNames.stay_duration)))

        return stays

    def filter_zero_dps_durations(self, stays: DataFrame) -> DataFrame:
        """Filters out stay records that would result in zero Daily Permanence Score.

        Removes records where the total stay duration is less than half of a time slot's
        duration (1/24th of a day). This pre-filtering step optimizes performance by
        eliminating records that would not contribute to the final score.

        Args:
            stays (DataFrame): DataFrame containing stay records with durations

        Returns:
            DataFrame: Filtered stay records above minimum duration threshold
        """
        window_spec = Window.partitionBy(
            ColNames.user_id,
            ColNames.user_id_modulo,
            ColNames.time_slot_initial_time,
        )

        # Count distinct cell_id within each window
        stays = stays.withColumn(
            "distinct_cell_count", F.size(F.collect_set(ColNames.cell_id).over(window_spec))
        ).withColumn("duration_sum", F.sum(ColNames.stay_duration).over(window_spec))

        # remove all records where DPS will be 0
        stays = stays.filter(
            F.col("duration_sum") >= F.lit(int((timedelta(days=1) / self.time_slot_number).total_seconds() / 2))
        ).drop("duration_sum")

        return stays

    def calculate_dps(self, stay_intervals: DataFrame, cell_footprint: DataFrame) -> DataFrame:
        """Calculate Daily Permanence Score (DPS) from user stay intervals.

        Processes stay intervals to determine user's presence in grid locations:
        - For multiple cell stays: explodes footprints and aggregates overlapping areas
        - For single cell stays: directly maps to corresponding grid IDs
        - For unknown locations: assigns special unknown identifier

        Args:
            stay_intervals (DataFrame): User stay intervals with duration and cell information
            cell_footprint (DataFrame): Mapping between cells and their grid coverage areas

        Returns:
            DataFrame: Daily Permanence Score results with grid IDs and type identification
        """

        # for intervals with multiple cells, we have to explode footprints to calculate sum of durations for grids
        # in case if there is overalap in coverage areas.
        # TODO: this is the most expensive part
        local_stay_intervals = stay_intervals.filter(F.col("is_abroad") == False)
        stay_intervals_multiple_cells = local_stay_intervals.filter(F.col("distinct_cell_count") > 1)

        stay_intervals_multiple_cells = self.join_footprints(
            stay_intervals_multiple_cells, cell_footprint, ["grid_ids"]
        )

        stay_intervals_multiple_cells = (
            stay_intervals_multiple_cells.withColumn(ColNames.grid_id, F.explode(F.col("grid_ids")))
            .drop("grid_ids")
            .groupBy(
                ColNames.user_id,
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
                ColNames.grid_id,
                ColNames.user_id_modulo,
            )
            .agg(F.sum(ColNames.stay_duration).alias(ColNames.stay_duration))
            .filter(
                F.col(ColNames.stay_duration)
                >= F.lit(int((timedelta(days=1) / self.time_slot_number).total_seconds() / 2))
            )
        )

        stay_intervals_multiple_cells = (
            stay_intervals_multiple_cells.groupBy(
                ColNames.user_id,
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
                ColNames.user_id_modulo,
            )
            .agg(F.array_sort(F.collect_set("grid_id")).alias(ColNames.dps))
            .select(
                ColNames.user_id,
                ColNames.dps,
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
                ColNames.user_id_modulo,
                F.lit(UeGridIdType.GRID_STR).alias(ColNames.id_type),
            )
        )

        # For intervals with single cell no need to explode, just join grid ids as is
        stay_intervals_single_cell = local_stay_intervals.filter(
            (F.col("distinct_cell_count") == 1)
            & (F.col(ColNames.cell_id) != F.lit(UeGridIdType.UNKNOWN).cast(StringType()))
        )
        stay_intervals_single_cell = self.join_footprints(stay_intervals_single_cell, cell_footprint, ["grid_ids"])

        stay_intervals_single_cell = stay_intervals_single_cell.select(
            ColNames.user_id,
            F.col("grid_ids").alias(ColNames.dps),
            ColNames.time_slot_initial_time,
            ColNames.time_slot_end_time,
            ColNames.user_id_modulo,
            F.lit(UeGridIdType.GRID_STR).alias(ColNames.id_type),
        )

        uknown_intervals = local_stay_intervals.filter(F.col(ColNames.cell_id) == F.lit(UeGridIdType.UNKNOWN))

        uknown_intervals = uknown_intervals.withColumn(
            ColNames.dps,
            F.array(F.lit(UeGridIdType.UNKNOWN).cast(LongType())),
        ).select(
            ColNames.user_id,
            ColNames.dps,
            ColNames.time_slot_initial_time,
            ColNames.time_slot_end_time,
            ColNames.user_id_modulo,
            F.lit(UeGridIdType.UKNOWN_STR).alias(ColNames.id_type),
        )

        abroad_intervals = stay_intervals.filter(
            (F.col("is_abroad") == True)
            & (
                F.col(ColNames.stay_duration)
                >= F.lit(int((timedelta(days=1) / self.time_slot_number).total_seconds() / 2))
            )
        )
        abroad_intervals = abroad_intervals.withColumn(
            ColNames.dps,
            F.array(F.col(ColNames.cell_id).cast(LongType())),
        ).select(
            ColNames.user_id,
            ColNames.dps,
            ColNames.time_slot_initial_time,
            ColNames.time_slot_end_time,
            ColNames.user_id_modulo,
            F.lit(UeGridIdType.ABROAD_STR).alias(ColNames.id_type),
        )

        dps = (
            stay_intervals_single_cell.union(stay_intervals_multiple_cells)
            .union(uknown_intervals)
            .union(abroad_intervals)
        )

        return dps

    def join_footprints(self, events: DataFrame, cell_footprints: DataFrame, columns: List) -> DataFrame:
        """
        Join the events dataframe with the cell_footprint dataframe.

        Args:
            events (DataFrame): events dataframe.
            cell_footprint (DataFrame): cell_footprint dataframe.

        Returns:
            DataFrame: events dataframe with the cell footprint information.
        """
        events = events.join(
            cell_footprints.select(
                F.col(ColNames.cell_id).alias("footprints_cell_id"),
                F.col(ColNames.year).alias("footprints_year"),
                F.col(ColNames.month).alias("footprints_month"),
                F.col(ColNames.day).alias("footprints_day"),
                *columns,
            ),
            (F.col(ColNames.cell_id) == F.col("footprints_cell_id"))
            & (F.col(ColNames.year) == F.col("footprints_year"))
            & (F.col(ColNames.month) == F.col("footprints_month"))
            & (F.col(ColNames.day) == F.col("footprints_day")),
            "left",
        ).drop(
            "footprints_cell_id",
            "footprints_year",
            "footprints_month",
            "footprints_day",
        )

        return events
