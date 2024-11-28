"""
Module that implements the Continuous Time Segmentations functionality
"""

from datetime import datetime, timedelta, time, date
from functools import partial
from typing import List, Optional, Tuple, Dict

import pandas as pd
from pandas import DataFrame as pdDataFrame

from pyspark.sql import DataFrame
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
from multimno.core.spark_session import check_if_data_path_exists
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames


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
                StructField(ColNames.time_segment_id, IntegerType()),
                StructField(ColNames.user_id, StringType()),
                StructField(ColNames.mcc, ShortType()),
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
        self.silver_signal_strength_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "time_segments_silver")
        self.output_data_objects[SilverTimeSegmentsDataObject.ID] = SilverTimeSegmentsDataObject(
            self.spark,
            self.silver_signal_strength_path,
        )

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
                .select(ColNames.overlapping_cell_ids)
                .withColumnRenamed(ColNames.overlapping_cell_ids, ColNames.cells)
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
        groups_sdf = self.current_interesection_groups_sdf.withColumn(
            ColNames.cells, F.concat_ws(",", F.col(ColNames.cells))
        )

        # Initialize an empty set
        intersections_set = set()

        # Iterate over each Row object and add the 'cells' value to the set
        for row in groups_sdf.collect():
            intersections_set.add(row.cells)

        # Broadcast the intersection groups to all the workers
        # TODO: To test this approach with large datasets, might not be feasible
        intersections_set = self.spark.sparkContext.broadcast(intersections_set)

        # Partial function to pass the current date and other parameters to the aggregation function
        aggregate_stays_partial = partial(
            self.aggregate_stays,
            current_date=self.current_date,
            min_time_stay=self.min_time_stay,
            max_time_missing_stay=self.max_time_missing_stay,
            max_time_missing_move=self.max_time_missing_move,
            pad_time=self.pad_time,
            intersections_set=intersections_set,
        )

        groupby_cols = self.input_data_objects[SilverEventFlaggedDataObject.ID].PARTITION_COLUMNS + [ColNames.user_id]

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

        self.current_segments_sdf = current_segments_sdf.cache()
        self.current_segments_sdf.count()

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
        intersections_set: set,
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

        current_date_start = datetime.combine(current_date, time())
        current_date_end = datetime.combine(current_date, time(23, 59, 59))
        previous_date_start = current_date_start - timedelta(days=1)
        previous_date_end = current_date_end - timedelta(days=1)

        # pdf = pdf.sort_values(by=[ColNames.timestamp])

        # Depending on the presence of events and last segments, initialize the user info and the last time segment
        # If there are no events, but last segment exists use the last time segment to derrive the user info
        # and create 'unknown' segment for current date
        # If there are events, use the first event to derrive the user info
        # if there are no last segments, assume last segment as 'unknown' for the whole previous date
        # if there are last segments, use the last segment
        user_id, user_mod, mcc, current_ts = ContinuousTimeSegmentation.initialize_user_and_ts(
            pdf,
            last_segments_pdf,
            current_date_start,
            current_date_end,
            previous_date_start,
            previous_date_end,
        )
        # We process events only if there are any
        if not pdf.empty:
            for event in pdf.itertuples(index=False):
                next_ts = {}
                ts_to_add = []
                event_timestamp = event.timestamp
                event_cell = event.cell_id

                # For the first time segment to start, look at the previous day's last segment
                # and create a new time segment with the same state starting from the day start till the first event
                if is_first_ts:
                    next_ts = ContinuousTimeSegmentation.handle_first_segment(
                        current_ts,
                        event_timestamp,
                        current_date_start,
                        max_time_missing_stay,
                        max_time_missing_move,
                        pad_time,
                    )
                    current_ts = next_ts
                    current_ts[ColNames.time_segment_id] = 1
                    is_first_ts = False

                current_intersection = list(set(current_ts[ColNames.cells] + [event_cell]))
                is_intersected = ContinuousTimeSegmentation.check_intersection(
                    current_ts[ColNames.cells], current_intersection, intersections_set
                )

                if current_ts[ColNames.state] == "unknown":
                    # If the current state is 'unknown' (from the previous day),
                    # create a new 'undetermined' time segment with pad_time adjustment
                    next_ts = ContinuousTimeSegmentation.create_time_segment(
                        current_ts[ColNames.end_timestamp],
                        event_timestamp,
                        [event_cell],
                        "undetermined",
                        current_ts[ColNames.time_segment_id],
                    )
                    ts_to_add = [current_ts]
                    current_ts = next_ts

                elif is_intersected and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_stay:
                    # If there's an intersection, check if we should update the current_ts or create a new one
                    if current_ts[ColNames.state] in ["undetermined", "stay"]:
                        # If the current state is 'undetermined' or 'stay' and the time gap is within
                        # the acceptable range for a stay update the current time segment with the new cell
                        # and the new end timestamp and set state to stay
                        current_ts[ColNames.end_timestamp] = event_timestamp
                        current_ts[ColNames.cells] = current_intersection
                        if event_timestamp - current_ts[ColNames.start_timestamp] > min_time_stay:
                            current_ts[ColNames.state] = "stay"
                    elif current_ts[ColNames.state] == "move":
                        # If the current state is 'move' and the time gap is within the acceptable range
                        # create new time segment with state 'undetermined' after move segment
                        next_ts = ContinuousTimeSegmentation.create_time_segment(
                            current_ts[ColNames.end_timestamp],
                            event_timestamp,
                            [event_cell],
                            "undetermined",
                            current_ts[ColNames.time_segment_id],
                        )
                        # if time gap is big enough to assume that its stay change the state to stay
                        if next_ts[ColNames.end_timestamp] - next_ts[ColNames.start_timestamp] > min_time_stay:
                            next_ts[ColNames.state] = "stay"
                        ts_to_add = [current_ts]
                        current_ts = next_ts

                elif (
                    not is_intersected and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_move
                ):
                    # If there's no intersection and the time gap is within the acceptable range for a move
                    mid_point = (
                        current_ts[ColNames.end_timestamp] + (event_timestamp - current_ts[ColNames.end_timestamp]) / 2
                    )

                    next_ts_1 = ContinuousTimeSegmentation.create_time_segment(
                        current_ts[ColNames.end_timestamp],
                        mid_point,
                        current_ts[ColNames.cells],
                        "move",
                        current_ts[ColNames.time_segment_id],
                    )

                    next_ts_2 = ContinuousTimeSegmentation.create_time_segment(
                        mid_point,
                        event_timestamp,
                        [event_cell],
                        "move",
                        next_ts_1[ColNames.time_segment_id],
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
                        current_ts[ColNames.time_segment_id],
                    )

                    next_ts_2 = ContinuousTimeSegmentation.create_time_segment(
                        event_timestamp - pad_time,
                        event_timestamp,
                        [event_cell],
                        "undetermined",
                        next_ts_1[ColNames.time_segment_id],
                    )

                    ts_to_add = [current_ts, next_ts_1]
                    current_ts = next_ts_2

                segments.extend(ts_to_add)

        # TODO: NOT IMPLEMENTED.
        # Currently there is no methodological description on how to handle the time from the last event to the end of the day.
        # Create extra time segment that covers the duration from the last event-based time segment until the end of the date.
        # if (current_ts[ColNames.end_timestamp] < current_date_end):
        #     last_ts = ContinuousTimeSegmentation.create_time_segment(
        #                     current_ts[ColNames.end_timestamp],
        #                     current_date_end,
        #                     [],
        #                     "unknown",
        #                     current_ts[ColNames.time_segment_id],
        #                 )
        #     last_ts[ColNames.is_last] = True
        #     segments.append(last_ts)
        # else:
        #     # If no extra time segment was generated, the final event-generated time segment is the last of the date.
        #     current_ts[ColNames.is_last] = True

        # Add final event-generated time segment to output list.
        current_ts[ColNames.is_last] = True
        segments.append(current_ts)

        # Prepare return columns
        segments_df = pd.DataFrame(segments)
        segments_df[ColNames.user_id] = user_id
        segments_df[ColNames.mcc] = mcc
        segments_df[ColNames.user_id_modulo] = user_mod

        return segments_df

    @staticmethod
    def handle_first_segment(
        current_ts: Dict,
        event_timestamp: datetime,
        current_date_start: datetime,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        pad_time: timedelta,
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
        current_date_start (datetime): The start of the current date.
        max_time_missing_stay (timedelta): The maximum time gap to consider continuation of a 'stay'.
        max_time_missing_move (timedelta): The maximum time gap to consider continuation of a 'move'.
        pad_time (timedelta): The padding time to have between 'unknown' segment.

        Returns:
        dict: The new first time segment for the current date.
        """

        if (
            current_ts[ColNames.state] in ["undetermined", "stay"]
            and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_stay
        ):
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp,
                list(current_ts[ColNames.cells]),
                current_ts[ColNames.state],
                current_ts[ColNames.time_segment_id],
            )
        elif (
            current_ts[ColNames.state] == "move"
            and event_timestamp - current_ts[ColNames.end_timestamp] <= max_time_missing_move
        ):
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp,
                list(current_ts[ColNames.cells]),
                current_ts[ColNames.state],
                current_ts[ColNames.time_segment_id],
            )
        else:
            pad_time = timedelta(seconds=0) if event_timestamp - current_date_start < pad_time else pad_time
            next_ts = ContinuousTimeSegmentation.create_time_segment(
                current_date_start,
                event_timestamp - pad_time,
                [],
                "unknown",
                current_ts[ColNames.time_segment_id],
            )

        return next_ts

    @staticmethod
    def create_time_segment(
        start_timestamp: datetime,
        end_timestamp: datetime,
        cells: List[str],
        state: str,
        previous_segment_id: Optional[int] = None,
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

        previous_segment_id = previous_segment_id if previous_segment_id else 0

        return {
            ColNames.time_segment_id: previous_segment_id + 1,
            ColNames.start_timestamp: start_timestamp,
            ColNames.end_timestamp: end_timestamp,
            ColNames.cells: cells,
            ColNames.state: state,
            ColNames.is_last: False,
        }

    @staticmethod
    def initialize_user_and_ts(
        pdf: pdDataFrame,
        last_segments_pdf: pdDataFrame,
        current_date_start: datetime,
        current_date_end: datetime,
        previous_date_start: datetime,
        previous_date_end: datetime,
    ) -> Tuple[str, int, str, Dict]:
        """
        Initializes the user ID, user ID modulo, MCC, and last time segment for time segmenation.

        If the events DataFrame for the current date is empty, it uses the user ID, user ID modulo,
            and MCC from the last segments DataFrame and creates a new 'unknown' time segment for the current date.

        If the events DataFrame for the current date is not empty, it uses the user ID, user ID modulo,
        and MCC from this DataFrame. If the last segments DataFrame is not empty, it uses the first
        time segment from this DataFrame as the last time segment. If the last segments DataFrame is empty,
        it creates a new 'unknown' time segment for the previous date.

        Parameters:
        pdf (pdDataFrame): The input Pandas DataFrame for the current date.
        last_segments_pdf (pdDataFrame): A Pandas DataFrame containing the last segments.
        current_date_start (datetime): The start of the current date.
        current_date_end (datetime): The end of the current date.
        previous_date_start (datetime): The start of the previous date.
        previous_date_end (datetime): The end of the previous date.

        Returns:
        Tuple[str, int, str, Dict]: The user ID, user ID modulo, MCC, and last time segment.
        """

        if pdf.empty:
            user_id = last_segments_pdf[ColNames.user_id][0]
            user_id_mod = last_segments_pdf[ColNames.user_id_modulo][0]
            mcc = last_segments_pdf[ColNames.mcc][0]
            last_time_segment = ContinuousTimeSegmentation.create_time_segment(
                current_date_start, current_date_end, [], "unknown"
            )
        else:
            user_id = pdf[ColNames.user_id][0]
            user_id_mod = pdf[ColNames.user_id_modulo][0]
            mcc = pdf[ColNames.mcc][0]
            if not last_segments_pdf.empty:
                last_time_segment = last_segments_pdf.iloc[0][
                    [
                        ColNames.time_segment_id,
                        ColNames.start_timestamp,
                        ColNames.end_timestamp,
                        ColNames.cells,
                        ColNames.state,
                    ]
                ].to_dict()
            else:
                last_time_segment = ContinuousTimeSegmentation.create_time_segment(
                    previous_date_start, previous_date_end, [], "unknown"
                )

        return user_id, user_id_mod, mcc, last_time_segment

    @staticmethod
    def check_intersection(
        previous_ts_inersection: List[str],
        current_intersection: List[str],
        intersections_set: set,
    ) -> bool:
        """
        Checks if there is an intersection between the current and previous time segments.

        This method takes two lists of cells, one for the previous time segment and one for the current time segment,
        and a Pandas DataFrame of intersections.

        If the list for the previous time segment is empty, it returns False, indicating that there is no intersection.

        If the list for the previous time segment is not empty, it checks if the sorted, comma-separated string of
        cells for the current time segment is in the DataFrame of intersections. If the list for the current time
        segment has more than one cell, it returns the result of this check. If the list for the current time segment
        has one cell, it returns True, indicating that there is an intersection.

        Parameters:
        previous_ts_inersection (List[str]): The cells of the previous time segment.
        current_intersection (List[str]): The cells of the current time segment.
        intersection_pd_df (pdDataFrame): A Pandas DataFrame containing the intersections.

        Returns:
        bool: True if there is an intersection, False otherwise.
        """
        if len(previous_ts_inersection) == 0:
            is_intersected = False
        else:
            is_intersected = (
                ",".join(sorted(current_intersection)) in intersections_set.value
                if len(current_intersection) > 1
                else True
            )
        return is_intersected
