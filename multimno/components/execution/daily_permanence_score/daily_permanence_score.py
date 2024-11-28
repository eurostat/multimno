"""
Module that implements the Daily Permanence Score functionality
"""

import re
import itertools
from typing import List, Tuple, Set

from datetime import datetime, timedelta, date
from pyspark.sql import DataFrame, Window

import pyspark.sql.functions as F
from sedona.sql import st_functions as STF
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, IntegerType, ShortType, ByteType
from multimno.core.component import Component

from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.grid import InspireGridGenerator
from multimno.core.log import get_execution_stats


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
        self.max_speed_thresh = self.config.getfloat(self.COMPONENT_ID, "max_speed_thresh")

        self.event_error_flags_to_include = self.config.geteval(self.COMPONENT_ID, "event_error_flags_to_include")

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        self.grid_gen = InspireGridGenerator(self.spark)
        self.current_date = None
        self.previous_date = None
        self.next_date = None

        self.current_events = None
        self.previous_events = None
        self.next_events = None

        self.current_cell_footprint = None
        self.previous_cell_footprint = None
        self.next_cell_footprint = None

    def initalize_data_objects(self):
        input_events_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver_flagged")
        input_cell_footprint_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        output_dps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_data_silver")

        silver_events = SilverEventFlaggedDataObject(self.spark, input_events_silver_path)

        silver_cell_footprint = SilverCellFootprintDataObject(self.spark, input_cell_footprint_silver_path)

        silver_dps = SilverDailyPermanenceScoreDataObject(self.spark, output_dps_path)

        self.input_data_objects = {silver_events.ID: silver_events, silver_cell_footprint.ID: silver_cell_footprint}

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
        self.assert_needed_dates_data_object(SilverEventFlaggedDataObject.ID, needed_dates)

        # Assert needed dates in cell footprint data:
        self.assert_needed_dates_data_object(SilverCellFootprintDataObject.ID, needed_dates)

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

    def filter_events(self, current_date: date) -> DataFrame:
        """
        Load events with no errors for a specific date.

        Args:
            current_date (date): current date.

        Returns:
            DataFrame: filtered events dataframe.
        """
        return self.input_data_objects[SilverEventFlaggedDataObject.ID].df.filter(
            (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            & (F.col(ColNames.error_flag).isin(self.event_error_flags_to_include))
        )

    def filter_cell_footprint(self, current_date: date) -> DataFrame:
        """
        Load cell footprints for a specific date.

        Args:
            current_date (date): current date.

        Returns:
            DataFrame: filtered cell footprint dataframe.
        """
        return self.input_data_objects[SilverCellFootprintDataObject.ID].df.filter(
            (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
        )

    # ------------------ Execute ------------------
    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        self.check_needed_dates()
        for current_date in self.data_period_dates:
            self.logger.info(current_date)
            self.logger.info(f"Processing events for {current_date.strftime('%Y-%m-%d')}")

            self.current_date = current_date
            self.previous_date = current_date - timedelta(days=1)
            self.next_date = current_date + timedelta(days=1)

            self.current_events = self.filter_events(self.current_date)
            self.previous_events = self.filter_events(self.previous_date)
            self.next_events = self.filter_events(self.next_date)

            self.current_cell_footprint = self.filter_cell_footprint(self.current_date)
            self.previous_cell_footprint = self.filter_cell_footprint(self.previous_date)
            self.next_cell_footprint = self.filter_cell_footprint(self.next_date)

            self.transform()

            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # load users events (dates D-1, D, D+1):
        events = self.build_events_table()

        # load cell footprint (dates D-1, D, D+1):
        cell_footprint = self.build_cell_footprint_table()

        # build time slots dataframe (date D):
        time_slots = self.build_time_slots_table()

        # differentiate 'move' events:
        events = self.detect_move_events(events, cell_footprint)

        # Determine stay durations:
        stays = self.determine_stay_durations(events)

        # Assign stay time slot, assign duration to time slots and map to calculate DPS:
        dps = self.calculate_dps(stays, time_slots)

        dps = dps.repartition(*SilverDailyPermanenceScoreDataObject.PARTITION_COLUMNS)

        self.output_data_objects[SilverDailyPermanenceScoreDataObject.ID].df = dps

    # ------------------ Prepare view tables ------------------
    def build_events_table(self) -> DataFrame:
        """
        Load events data for date D, also adding last event of each
        user from date D-1 and first event of each user from D+1.

        Returns:
            DataFrame: events dataframe.
        """
        # reach last event from previous day:
        window = Window.partitionBy(ColNames.user_id).orderBy(F.desc(ColNames.timestamp))
        self.previous_events = (
            self.previous_events.withColumn("row_number", F.row_number().over(window))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        # reach first event from next day:
        window = Window.partitionBy(ColNames.user_id).orderBy(ColNames.timestamp)
        self.next_events = (
            self.next_events.withColumn("row_number", F.row_number().over(window))
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        # concat all events together (last of D-1 + all D + first of D+1):
        events = (
            self.previous_events.union(self.current_events)
            .union(self.next_events)
            .drop(ColNames.longitude, ColNames.latitude, ColNames.loc_error, ColNames.error_flag)
        )

        return events

    def build_cell_footprint_table(self) -> DataFrame:
        """
        Load cell footprint data for dates D-1, D and D+1.

        Returns:
            DataFrame: cell footprint dataframe.
        """
        cell_footprint = self.previous_cell_footprint.union(self.current_cell_footprint).union(self.next_cell_footprint)

        cell_footprint = self.grid_gen.grid_ids_to_centroids(cell_footprint)

        cell_footprint = cell_footprint.groupBy([ColNames.cell_id, ColNames.year, ColNames.month, ColNames.day]).agg(
            F.collect_list(ColNames.geometry).alias(ColNames.geometry),
            # F.collect_list(ColNames.grid_id).alias("grid_ids"),
        )

        cell_footprint = cell_footprint.withColumn(
            ColNames.geometry,
            STF.ST_ConcaveHull(STF.ST_Collect(F.col(ColNames.geometry)), 0.5),
        )

        return cell_footprint

    def build_time_slots_table(self) -> DataFrame:
        """
        Build a dataframe with the specified time slots for the current date.

        Returns:
            DataFrame: time slots dataframe.
        """
        time_slot_length = timedelta(days=1) / self.time_slot_number

        time_slots_list = []
        previous_end_time = datetime(
            year=self.current_date.year,
            month=self.current_date.month,
            day=self.current_date.day,
            hour=0,
            minute=0,
            second=0,
        )

        while previous_end_time.date() == self.current_date:
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

        return self.spark.createDataFrame(time_slots_list, schema=schema)

    # ------------------ Main transformations ------------------
    def detect_move_events(self, events: DataFrame, cell_footprint: DataFrame) -> DataFrame:
        """
        Detect which of the events are associated to moves according to the
        distances/times from previous to posterior event and a speed threshold.

        Args:
            events (DataFrame): events dataframe.
            cell_footprint (DataFrame): cells footprint dataframe.

        Returns:
            DataFrame: events dataframe, with an additional 'is_move' boolean column.
        """
        # inner join -> bring cell footprints to events data discarding events for which there is no cell footprint
        events = events.join(
            cell_footprint.select(ColNames.cell_id, ColNames.year, ColNames.month, ColNames.day, ColNames.geometry),
            (events[ColNames.cell_id] == cell_footprint[ColNames.cell_id])
            & (events[ColNames.year] == cell_footprint[ColNames.year])
            & (events[ColNames.month] == cell_footprint[ColNames.month])
            & (events[ColNames.day] == cell_footprint[ColNames.day]),
            "inner",
        ).drop(
            cell_footprint[ColNames.cell_id],
            cell_footprint[ColNames.year],
            cell_footprint[ColNames.month],
            cell_footprint[ColNames.day],
        )

        # Add lags of timestamp, cell_id and grid_ids:
        window = Window.partitionBy(ColNames.user_id).orderBy(ColNames.timestamp)
        lag_fields = [ColNames.timestamp, ColNames.cell_id, ColNames.geometry]
        for lf in lag_fields:
            events = events.withColumn(f"{lf}_+1", F.lag(lf, -1).over(window)).withColumn(
                f"{lf}_-1", F.lag(lf, 1).over(window)
            )

        # Calculate distance between grid tiles associated to events -1, 0 and +1:
        # Calculate speeds and determine which rows are moves:
        events = (
            events.withColumn(
                "dist_0_+1",
                STF.ST_Distance(F.col(ColNames.geometry), F.col(f"{ColNames.geometry}_+1")),
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
                STF.ST_Distance(F.col(f"{ColNames.geometry}_-1"), F.col(f"{ColNames.geometry}_+1")),
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
            .withColumn(
                "threshold_-1",
                F.when(
                    (F.col("cell_id") == F.col("cell_id_-1"))
                    & (F.col("timestamp_-1") >= night_start_time)
                    & (F.col("timestamp") <= night_end_time),
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
                    F.col(ColNames.timestamp) - F.col(f"{ColNames.timestamp}_-1") <= F.col("threshold_-1"),
                    F.col(ColNames.timestamp) - (F.col(ColNames.timestamp) - F.col(f"{ColNames.timestamp}_-1")) / 2,
                ).otherwise(F.col(ColNames.timestamp) - self.max_time_thresh / 2),
            )
            .withColumn(
                "end_time",
                F.when(
                    F.col(f"{ColNames.timestamp}_+1") - F.col(ColNames.timestamp) <= F.col("threshold_+1"),
                    F.col(f"{ColNames.timestamp}_+1")
                    - (F.col(f"{ColNames.timestamp}_+1") - F.col(ColNames.timestamp)) / 2,
                ).otherwise(F.col(ColNames.timestamp) + self.max_time_thresh / 2),
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

    def calculate_dps(self, stays: DataFrame, time_slots: DataFrame) -> DataFrame:
        """
        Temporally intersect each stay interval with the specified time slots. Then
        calculate the number of seconds that each user stays at each grid tile within
        each of these time slots according to the stay intervals and the grid tiles
        associated to each stay.

        Args:
            stays (DataFrame): stays dataframe.
            time_slots (DataFrame): time slots dataframe.

        Returns:
            DataFrame: daily permanence score dataframe.
        """
        dps = (
            stays.crossJoin(time_slots)
            .withColumn("int_init_time", F.greatest(F.col("init_time"), F.col(ColNames.time_slot_initial_time)))
            .withColumn("int_end_time", F.least(F.col("end_time"), F.col(ColNames.time_slot_end_time)))
            .withColumn(
                ColNames.stay_duration,
                F.when(
                    F.col("int_init_time") < F.col("int_end_time"),
                    F.unix_timestamp(F.col("int_end_time")) - F.unix_timestamp(F.col("int_init_time")),
                ).otherwise(0),
            )
            .drop("int_init_time", "int_end_time", "init_time", "end_time")
        )

        unknown_dps = (
            dps.groupby(
                ColNames.user_id_modulo, ColNames.user_id, ColNames.time_slot_initial_time, ColNames.time_slot_end_time
            )
            .agg(
                (
                    F.lit((timedelta(days=1) / self.time_slot_number).total_seconds()).cast(IntegerType())
                    - F.sum(ColNames.stay_duration).cast(FloatType())
                ).alias(ColNames.stay_duration)
            )
            .filter(F.col(ColNames.stay_duration) > 0.0)
            .select(
                ColNames.user_id,
                ColNames.user_id_modulo,
                F.lit("unknown").alias(ColNames.cell_id),
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
                ColNames.stay_duration,
                F.lit("unknown").alias(ColNames.id_type),
            )
        )

        known_dps = (
            dps.filter(F.col(ColNames.stay_duration) > 0.0)
            .groupby(
                ColNames.user_id,
                ColNames.user_id_modulo,
                ColNames.cell_id,
                ColNames.time_slot_initial_time,
                ColNames.time_slot_end_time,
            )
            .agg(F.sum(ColNames.stay_duration).cast(FloatType()).alias(ColNames.stay_duration))
            .withColumn(ColNames.id_type, F.lit("cell"))
        )

        dps = (
            known_dps.union(unknown_dps)
            # since some stays may come from events in previous date, fix and always set current date:
            .withColumn(ColNames.year, F.lit(self.current_date.year).cast(ShortType()))
            .withColumn(ColNames.month, F.lit(self.current_date.month).cast(ByteType()))
            .withColumn(ColNames.day, F.lit(self.current_date.day).cast(ByteType()))
        )

        return dps
