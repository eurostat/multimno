"""
Module that implements the Continuous Time Segmentations functionality
"""

from datetime import datetime, timedelta, time, date
from functools import partial
from typing import List, Optional, Tuple, Dict
import hashlib
import pandas as pd
from pandas import DataFrame as pdDataFrame

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    ArrayType,
    BooleanType,
    IntegerType,
    ShortType,
)
from multimno.core.component import Component

from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.data_objects.silver.silver_time_segments_data_object import (
    SilverTimeSegmentsDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.log import get_execution_stats


class ContinuousTimeSegmentation(Component):
    """
    A class to aggregate events into time segments.
    """

    COMPONENT_ID = "ContinuousTimeSegmentation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.min_time_stay = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "min_time_stay_s"))

        self.max_time_missing_stay = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_missing_stay_s"))

        self.max_time_missing_move = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "max_time_missing_move_s"))

        self.pad_time = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "pad_time_s"))

        self.event_error_flags_to_include = self.config.geteval(self.COMPONENT_ID, "event_error_flags_to_include")
        # this is for UDF
        self.segmentation_return_schema = StructType(
            [
                StructField(ColNames.start_timestamp, TimestampType()),
                StructField(ColNames.end_timestamp, TimestampType()),
                StructField(ColNames.cells, ArrayType(StringType())),
                StructField(ColNames.state, StringType()),
                StructField(ColNames.is_last, BooleanType()),
                StructField(ColNames.time_segment_id, StringType()),
                StructField(ColNames.user_id, StringType()),
                StructField(ColNames.mcc, ShortType()),
                StructField(ColNames.mnc, StringType()),
                StructField(ColNames.plmn, IntegerType()),
                StructField(ColNames.user_id_modulo, IntegerType()),
            ]
        )

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

    def initalize_data_objects(self):

        # Input
        self.input_data_objects = {}
        self.is_first_run = self.config.getboolean(self.COMPONENT_ID, "is_first_run")

        inputs = {
            "event_data_silver_flagged": SilverEventFlaggedDataObject,
            "cell_intersection_groups_data_silver": SilverCellIntersectionGroupsDataObject,
        }
        if not self.is_first_run:
            inputs["time_segments_silver"] = SilverTimeSegmentsDataObject

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # Output
        self.output_data_objects = {}
        self.output_silver_time_segments_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "time_segments_silver")
        self.output_data_objects[SilverTimeSegmentsDataObject.ID] = SilverTimeSegmentsDataObject(
            self.spark,
            self.output_silver_time_segments_path,
        )

        # Output clearing
        clear_destination_directory = self.config.getboolean(
            self.COMPONENT_ID, "clear_destination_directory", fallback=False
        )
        if clear_destination_directory:
            delete_file_or_folder(self.spark, self.output_silver_time_segments_path)
        # TODO add optional date-limited deletion when not first run,
        # but consider that segments get generated for an additional one day before the starting date

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        # If segements was already calculated and this is continuation of the previous run
        # we need to get the last time segment for each user.
        # If this is the first run, we will create an empty dataframe
        if self.is_first_run:
            self.intital_time_segment = self.spark.createDataFrame([], SilverTimeSegmentsDataObject.SCHEMA)
        else:
            previous_date = self.data_period_start - timedelta(days=1)
            self.intital_time_segment = self.input_data_objects[SilverTimeSegmentsDataObject.ID].df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(previous_date))
                & (F.col(ColNames.is_last) == True)
            )

            # this is needed to join the last time segement with the events of the current date
            self.intital_time_segment = self.intital_time_segment.withColumn(
                ColNames.start_timestamp, F.date_add(F.col(ColNames.start_timestamp), 1)
            ).withColumns(
                {
                    ColNames.year: F.year(ColNames.start_timestamp).cast("smallint"),
                    ColNames.month: F.month(ColNames.start_timestamp).cast("tinyint"),
                    ColNames.day: F.dayofmonth(ColNames.start_timestamp).cast("tinyint"),
                }
            )

            self.intital_time_segment = self.intital_time_segment.withColumn(
                ColNames.user_id, F.hex(F.col(ColNames.user_id))
            )

        # for every date in the data period, get the events and the intersection groups
        # for that date and calculate the time segments
        for current_date in self.data_period_dates:

            self.logger.info(f"Processing events for {current_date.strftime('%Y-%m-%d')}")

            self.current_date = current_date
            self.current_input_events_sdf = self.input_data_objects[SilverEventFlaggedDataObject.ID].df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
                & (F.col(ColNames.error_flag).isin(self.event_error_flags_to_include))
            )

            self.current_interesection_groups_sdf = (
                self.input_data_objects[SilverCellIntersectionGroupsDataObject.ID]
                .df.filter(
                    (
                        F.make_date(
                            F.col(ColNames.year),
                            F.col(ColNames.month),
                            F.col(ColNames.day),
                        )
                        == F.lit(current_date)
                    )
                )
                .select(ColNames.cell_id, ColNames.overlapping_cell_ids, ColNames.year, ColNames.month, ColNames.day)
            )

            self.transform()
            self.write()
            self.current_segments_sdf.unpersist()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        current_events = self.current_input_events_sdf
        last_time_segment = self.intital_time_segment

        # TODO: This conversion is needed for Pandas serialisation/deserialisation,
        # to remove it when user_id will be stored as string, not as binary
        current_events = current_events.withColumn(ColNames.user_id, F.hex(F.col(ColNames.user_id)))
        # Conversion to string is needed for easier intersection groups lookup in the aggregation function
        intersections_groups_df = self.current_interesection_groups_sdf

        # Add overlapping_cell_ids list to each event
        current_events = (
            current_events.alias("df1")
            .join(
                intersections_groups_df.alias("df2"),
                on=[ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id],
                how="left",
            )
            .select(
                f"df1.{ColNames.user_id}",
                f"df1.{ColNames.timestamp}",
                f"df1.{ColNames.mcc}",
                f"df1.{ColNames.mnc}",
                f"df1.{ColNames.plmn}",
                f"df1.{ColNames.cell_id}",
                f"df1.{ColNames.year}",
                f"df1.{ColNames.month}",
                f"df1.{ColNames.day}",
                f"df1.{ColNames.user_id_modulo}",
                ColNames.overlapping_cell_ids,
            )
        )
        # TODO add first event(s?) from next date to current events to handle last segment of date

        # Partial function to pass the current date and other parameters to the aggregation function
        aggregate_stays_partial = partial(
            self.aggregate_stays,
            current_date=self.current_date,
            min_time_stay=self.min_time_stay,
            max_time_missing_stay=self.max_time_missing_stay,
            max_time_missing_move=self.max_time_missing_move,
            pad_time=self.pad_time,
            # intersections_set=intersections_set,
        )

        groupby_cols = [
            ColNames.user_id
        ]  # + self.input_data_objects[SilverEventFlaggedDataObject.ID].PARTITION_COLUMNS

        # TODO This filtration should be removed after bugfixes since it is not necessary
        #   if previous modules are run correctly, since each valid event should have a valid cell.
        current_events = current_events.where(F.col(ColNames.overlapping_cell_ids).isNotNull())

        # Using cogroup to join the current events with the last time segment.
        # Handy to avoid joining last segments to every row of the current events
        # Also helps to detect missing events for the user for the last day or for the current day
        # TODO: To test this approach with large datasets, might not be feasible
        current_segments_sdf = (
            current_events.groupby(*groupby_cols)
            .cogroup(last_time_segment.groupby(*groupby_cols))
            .applyInPandas(aggregate_stays_partial, self.segmentation_return_schema)
        )

        current_segments_sdf = current_segments_sdf.withColumns(
            {
                ColNames.year: F.year(ColNames.start_timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.start_timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.start_timestamp).cast("tinyint"),
            }
        )
        current_segments_sdf = current_segments_sdf.cache()
        self.current_segments_sdf = current_segments_sdf

        # Need to keep last segment for the next date iteration
        # Have to add one day to time columns to be able to cogroup with the next day
        last_segments = current_segments_sdf.filter(F.col(ColNames.is_last) == True)
        last_segments = last_segments.withColumn(
            ColNames.start_timestamp, F.date_add(F.col(ColNames.start_timestamp), 1)
        )

        last_segments = last_segments.withColumns(
            {
                ColNames.year: F.year(ColNames.start_timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.start_timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.start_timestamp).cast("tinyint"),
            }
        )
        self.intital_time_segment.unpersist()
        self.intital_time_segment = last_segments.cache()
        last_segments.count()

        # TODO: This conversion is needed to get back to binary after Pandas serialisation/deserialisation,
        # to remove it when user_id will be stored as string, not as binary
        current_segments_sdf = current_segments_sdf.withColumn(ColNames.user_id, F.unhex(F.col(ColNames.user_id)))

        current_segments_sdf = current_segments_sdf.select(
            *[field.name for field in SilverTimeSegmentsDataObject.SCHEMA.fields]
        )
        columns = {
            field.name: F.col(field.name).cast(field.dataType) for field in SilverTimeSegmentsDataObject.SCHEMA.fields
        }
        current_segments_sdf = current_segments_sdf.withColumns(columns)

        current_segments_sdf = current_segments_sdf.repartition(
            ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo
        ).sortWithinPartitions(ColNames.user_id, ColNames.start_timestamp)

        self.output_data_objects[SilverTimeSegmentsDataObject.ID].df = current_segments_sdf

    @staticmethod
    def aggregate_stays(
        pdf: pdDataFrame,
        last_segments_pdf: pdDataFrame,
        current_date: date,
        min_time_stay: timedelta,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        pad_time: timedelta,
    ) -> DataFrame:
        """
        Aggregates events into Time Segments for a given user.

        This method processes a Pandas DataFrame of user events, and aggregates them into time segments based on
        certain conditions. It handles the first event separately, then iterates over the remaining events. For each
        event, it checks if there's an intersection of an event cell and the current time segment cells and if the time
        gap is within anacceptable range. Depending on the state of the current time segment and the result of
        the intersection check, it either updates the current time segment or creates a new one.

        Input user event data is expected to be sorted by timestamp.

        Parameters:
        pdf (pdDataFrame): The input Pandas DataFrame containing user events to be processed.
        last_segments_pdf (pdDataFrame): A Pandas DataFrame containing the last segments.
        current_date (date): The current date.
        min_time_stay (timedelta): The minimum time to consider a segment as a 'stay'.
        max_time_missing_stay (timedelta): The maximum time gap to consider continuation of a 'stay'.
        max_time_missing_move (timedelta): The maximum time gap to consider continuation of a 'move'.
        pad_time (timedelta): The padding time to have between 'unknown' segment.
        groups_sdf (DataFrame): A PySpark DataFrame containing the groups.

        Returns:
        DataFrame: A DataFrame containing the aggregated time segments.
        """
        segments = []
        is_first_ts = True

        current_date_start = datetime.combine(current_date, time(0, 0, 0))
        current_date_end = datetime.combine(current_date, time(23, 59, 59))
        previous_date_start = current_date_start - timedelta(days=1)
        previous_date_end = current_date_end - timedelta(days=1)

        pdf = pdf.sort_values(by=[ColNames.timestamp])

        # The user has current date events, previous time segment, or both. This code is not executed if both are missing.
        # If previous time segment exists and events do exist, set it as current time segment.
        #   Then process events to generate segments.
        # If previous time segment exists and events do not exist, create whole-day "unknown" time segment.
        #   This is the only segment for the date.
        # If prevous time segment does not exist and events do exist, generate previous day whole-day "unknown" time segment using user info from events, set it as current time segment.
        #   Then process events to generate segments.

        if pdf.empty and not last_segments_pdf.empty:
            # If a previous segments exists but this date has no data, then
            # generate current date "unknown" segment using user data from previous date segment and add it to segments list.
            # Do not generate any other segments for the current date.
            user_id, user_mod, mcc, mnc, plmn = ContinuousTimeSegmentation.get_user_info_from_pdf(last_segments_pdf)
            current_date_only_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start, current_date_end, [], "unknown", user_id
            )
            current_date_only_ts[ColNames.is_last] = True
            segments.append(current_date_only_ts)
        else:  # there exist event records of this date
            if last_segments_pdf.empty:
                # There is no existing previous date segment, so this is the first date this user has data.
                # Generate previous date "unknown" segment, add it to segments list, set it as current segment.
                user_id, user_mod, mcc, mnc, plmn = ContinuousTimeSegmentation.get_user_info_from_pdf(pdf)
                current_ts = ContinuousTimeSegmentation.create_time_segment(
                    previous_date_start, previous_date_end, [], "unknown", user_id
                )
                current_ts[ColNames.is_last] = True
                segments.append(current_ts)
            else:
                # Get existing previous day segment, set as current segment.
                user_id, user_mod, mcc, mnc, plmn = ContinuousTimeSegmentation.get_user_info_from_pdf(pdf)
                current_ts = ContinuousTimeSegmentation.get_previous_date_last_segment(last_segments_pdf)
            # Iterate over events in chronological order to generate segments.
            for event in pdf.itertuples(index=False):
                next_ts = {}
                ts_to_add = []
                event_timestamp = event.timestamp
                event_cell = event.cell_id
                overlapping_cell_ids = event.overlapping_cell_ids

                # Add event's own cell_id to overlapping_cell_ids
                current_event_cell_overlapping_cell_ids = overlapping_cell_ids.tolist() + [event_cell]

                # For the first time segment to start, look at the previous day's last segment and
                # create a new time segment with the same state from the day start until the first event.
                if is_first_ts:
                    next_ts = ContinuousTimeSegmentation.handle_first_segment(
                        current_ts=current_ts,
                        event_timestamp=event_timestamp,
                        event_cell=event_cell,
                        current_date_start=current_date_start,
                        max_time_missing_stay=max_time_missing_stay,
                        max_time_missing_move=max_time_missing_move,
                        pad_time=pad_time,
                        user_id=user_id,
                    )
                    current_ts = next_ts
                    is_first_ts = False

                # Determine if this event intersects with the current time segment.
                is_intersected = ContinuousTimeSegmentation.check_intersection(
                    current_ts[ColNames.cells],
                    current_event_cell_overlapping_cell_ids,
                )

                if current_ts[ColNames.state] == "unknown":
                    # If the current state is 'unknown' (from the previous day),
                    # create a new 'undetermined' time segment with pad_time adjustment
                    next_ts = ContinuousTimeSegmentation.create_time_segment(
                        current_ts[ColNames.end_timestamp],
                        event_timestamp,
                        [event_cell],
                        "undetermined",
                        user_id,
                    )
                    ts_to_add = [current_ts]
                    current_ts = next_ts

                elif is_intersected and (event_timestamp - current_ts[ColNames.end_timestamp]) <= max_time_missing_stay:
                    # If there's an intersection, check if we should update the current_ts or create a new one
                    if current_ts[ColNames.state] in ["undetermined", "stay"]:
                        # If the current state is 'undetermined' or 'stay' and the time gap is within
                        # the acceptable range for a stay update the current time segment with the new cell
                        # and the new end timestamp and set state to stay
                        current_ts[ColNames.end_timestamp] = event_timestamp
                        current_ts[ColNames.cells] = list(set(current_ts[ColNames.cells] + [event_cell]))
                        if current_ts[ColNames.end_timestamp] - current_ts[ColNames.start_timestamp] > min_time_stay:
                            current_ts[ColNames.state] = "stay"
                    elif current_ts[ColNames.state] == "move":
                        # If the current state is 'move' and the time gap is within the acceptable range
                        # create new time segment with state 'undetermined' after 'move' segment
                        next_ts = ContinuousTimeSegmentation.create_time_segment(
                            current_ts[ColNames.end_timestamp],
                            event_timestamp,
                            [event_cell],
                            "undetermined",
                            user_id,
                        )
                        # if time gap is big enough to assume that it is a stay, change the state to 'stay'
                        if next_ts[ColNames.end_timestamp] - next_ts[ColNames.start_timestamp] > min_time_stay:
                            next_ts[ColNames.state] = "stay"
                        ts_to_add = [current_ts]
                        current_ts = next_ts

                elif (
                    not is_intersected and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_move
                ):
                    # If there is no intersection and the time gap is within the acceptable range for a move, create
                    # two 'move' segments, each covering half the duration of the time gap
                    mid_point = (
                        current_ts[ColNames.end_timestamp] + (event_timestamp - current_ts[ColNames.end_timestamp]) / 2
                    )

                    next_ts_1 = ContinuousTimeSegmentation.create_time_segment(
                        current_ts[ColNames.end_timestamp],
                        mid_point,
                        current_ts[ColNames.cells],
                        "move",
                        user_id,
                    )

                    next_ts_2 = ContinuousTimeSegmentation.create_time_segment(
                        mid_point,
                        event_timestamp,
                        [event_cell],
                        "move",
                        user_id,
                    )

                    ts_to_add = [current_ts, next_ts_1]
                    current_ts = next_ts_2

                else:
                    # If the time gap is too big, create 'unknown' segment for missing time with pad_time adjustment
                    # create new time segment with state 'undetermined' with pad_time adjustment
                    current_ts[ColNames.end_timestamp] = current_ts[ColNames.end_timestamp] + pad_time

                    next_ts_1 = ContinuousTimeSegmentation.create_time_segment(
                        current_ts[ColNames.end_timestamp],
                        event_timestamp - pad_time,
                        [],
                        "unknown",
                        user_id,
                    )

                    next_ts_2 = ContinuousTimeSegmentation.create_time_segment(
                        event_timestamp - pad_time,
                        event_timestamp,
                        [event_cell],
                        "undetermined",
                        user_id,
                    )

                    ts_to_add = [current_ts, next_ts_1]
                    current_ts = next_ts_2

                segments.extend(ts_to_add)
            # For the last ongoing segment, set is_last to true and add it as the last segment of the day.
            current_ts[ColNames.is_last] = True
            segments.append(current_ts)
        # TODO: NOT IMPLEMENTED.
        # Currently there is no methodological description on how to handle the time from the last event to the end of the day.
        # May need to create and extra time segment that covers the duration from the last event-based time segment until the end of the date.

        # Prepare return columns
        segments_df = pd.DataFrame(segments)
        segments_df[ColNames.user_id] = user_id
        segments_df[ColNames.mcc] = mcc
        segments_df[ColNames.mnc] = mnc
        segments_df[ColNames.plmn] = plmn
        segments_df[ColNames.user_id_modulo] = user_mod

        return segments_df

    @staticmethod
    def handle_first_segment(
        current_ts: Dict,
        event_timestamp: datetime,
        event_cell: int,
        current_date_start: datetime,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        pad_time: timedelta,
        user_id: str,
    ) -> dict:
        """
        Handles the first segment for a user for a date based on a previous date last segment.

        This method takes the last time segment of previous date and the timestamp of the first
            event in the current date.
        It checks the state of the current time segment and the time difference between the end of the current
        time segment and the first event in the next date.

        If the state is 'undetermined' or 'stay' and the time difference is within the maximum missing stay time,
        or if the state is 'move' and the time difference is within the maximum missing move time, it creates a new
        time segment with the same cells, state. The start timestamp
        of the new time segment is the start of the current date, and the end timestamp
            is the timestamp of the first event.

        If neither of these conditions are met, it creates a new time segment with
            an empty list of cells, state 'unknown'.
        The start timestamp of the new time segment is the start of the next date,
            and the end timestamp is the timestamp of the first event minus the padding time.

        Parameters:
        current_ts (Dict): The last time segment from previous date.
        event_timestamp (datetime): The timestamp of the first event in the current date.
        event_cell (int): The cell of the first event in the current date.
        current_date_start (datetime): The start of the current date.
        max_time_missing_stay (timedelta): The maximum time gap to consider continuation of a 'stay'.
        max_time_missing_move (timedelta): The maximum time gap to consider continuation of a 'move'.
        pad_time (timedelta): The padding time to have between 'unknown' segment.

        Returns:
        dict: The new first time segment for the current date.
        """
        # TODO Verify if logic is consistent with non-first segments
        if (
            current_ts[ColNames.state] in ["undetermined", "stay"]
            and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_stay
        ):
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp,
                list(set(current_ts[ColNames.cells] + [event_cell])),
                current_ts[ColNames.state],
                user_id,
            )
        elif (
            current_ts[ColNames.state] == "move"
            and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_move
        ):
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp,
                list(set(current_ts[ColNames.cells] + [event_cell])),
                current_ts[ColNames.state],
                user_id,
            )
        else:
            pad_time = timedelta(seconds=0) if event_timestamp - current_date_start < pad_time else pad_time
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp - pad_time,
                [],
                "unknown",
                user_id,
            )

        return next_ts

    @staticmethod
    def create_time_segment(
        start_timestamp: datetime,
        end_timestamp: datetime,
        cells: List[str],
        state: str,
        user_id: str,
    ) -> Dict:
        """
        Creates a new time segment.

        It creates a new time segment with these values, incrementing the segment ID by 1
        if a previous segment ID is provided, or setting it to 1 if not.

        Parameters:
        start_timestamp (datetime): The start timestamp of the time segment.
        end_timestamp (datetime): The end timestamp of the time segment.
        cells (List[str]): The cells of the time segment.
        state (str): The state of the time segment.
        previous_segment_id (Optional[int]): The ID of the previous time segment, if any.

        Returns:
        Dict: The new time segment.
        """
        segment_id_string = f"{user_id}{start_timestamp}"
        return {
            ColNames.time_segment_id: hashlib.md5(segment_id_string.encode()).hexdigest(),
            ColNames.start_timestamp: start_timestamp,
            ColNames.end_timestamp: end_timestamp,
            ColNames.cells: cells,
            ColNames.state: state,
            ColNames.is_last: False,
        }

    @staticmethod
    def get_user_info_from_pdf(pdf: pdDataFrame) -> Tuple[str, int, str]:
        """
        Gets user_id, user_id_modulo, mcc, mnc and plmn from Pandas DataFrame containing columns with the corresponding names.
        Values from the first row of the dataframe are used.

        Args:
            pdf (pdDataFrame): Pandas DataFrame

        Returns:
            Tuple[str, int, str]: user_id, user_id_modulo, mcc, mnc, plmn
        """
        user_id = pdf[ColNames.user_id][0]
        user_id_mod = pdf[ColNames.user_id_modulo][0]
        mcc = pdf[ColNames.mcc][0]
        mnc = pdf[ColNames.mnc][0]
        plmn = pdf[ColNames.plmn][0]
        return user_id, user_id_mod, mcc, mnc, plmn

    @staticmethod
    def get_previous_date_last_segment(last_segments_pdf: pdDataFrame) -> Dict:
        """
        Gets last segment from the previous date. The input DataFrame is expected to contain only one row to be retrieved.

        Args:
            last_segments_pdf (pdDataFrame): Pandas DataFrame containing the user's previous date's final segment.

        Returns:
            Dict: dict of segment
        """
        return last_segments_pdf.iloc[0][
            [
                ColNames.time_segment_id,
                ColNames.start_timestamp,
                ColNames.end_timestamp,
                ColNames.cells,
                ColNames.state,
            ]
        ].to_dict()

    @staticmethod
    def check_intersection(
        previous_ts_cells: List[str],
        current_event_overlapping_cell_ids: List[str],
    ) -> bool:
        """
        Checks if there is an intersection between the existing time segment and the current event.

        This method takes two lists of cells, one for the cells included in the existing time segment and the other for
        the overlapping cell ids of the current event's cell.
        The time segment intersects with the event if each of the time segment's cells are included in the event's overlapping cell ids list.

        A segment with no cells cannot intersect and returns False.

        Parameters:
        previous_ts_cells (List[str]): The cells of the existing time segment.
        current_event_overlapping_cell_ids (List[str]): Cells the current event's cell overlaps with, including itself.

        Returns:
        bool: True if there is an intersection, False otherwise.
        """
        if len(previous_ts_cells) == 0:
            is_intersected = False
        else:
            is_intersected = set(previous_ts_cells).issubset(set(current_event_overlapping_cell_ids))
        return is_intersected
