"""
Module that implements the Continuous Time Segmentations functionality
"""

from datetime import datetime, timedelta, time, date
from functools import partial
from typing import List, Optional, Tuple, Dict, Any
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
    ByteType,
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
from multimno.core.constants.columns import ColNames, SegmentStates
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


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
        self.max_time_missing_abroad = timedelta(
            seconds=self.config.getint(self.COMPONENT_ID, "max_time_missing_abroad_s")
        )
        self.pad_time = timedelta(seconds=self.config.getint(self.COMPONENT_ID, "pad_time_s"))

        self.event_error_flags_to_include = self.config.geteval(self.COMPONENT_ID, "event_error_flags_to_include")
        self.local_mcc = self.config.getint(self.COMPONENT_ID, "local_mcc")
        # this is for UDF
        self.segmentation_return_schema = StructType(
            [
                StructField(ColNames.start_timestamp, TimestampType()),
                StructField(ColNames.end_timestamp, TimestampType()),
                StructField(ColNames.cells, ArrayType(StringType())),
                StructField(ColNames.state, ByteType()),
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
        self.last_time_segments = None
        self.current_date = None

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

        # for every date in the data period, get the events and the intersection groups
        # for that date and calculate the time segments
        for current_date in self.data_period_dates:
            self.logger.info(f"Processing events for {current_date.strftime('%Y-%m-%d')}")
            self.current_date = current_date
            self.read()

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

            # If segements was already calculated and this is continuation of the previous run
            # we need to get the last time segment for each user.
            # If this is the first run, we will create an empty dataframe
            if not self.is_first_run:

                previous_date = current_date - timedelta(days=1)
                self.last_time_segments = self.input_data_objects[SilverTimeSegmentsDataObject.ID].df.filter(
                    (
                        F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                        == F.lit(previous_date)
                    )
                    & (F.col(ColNames.is_last) == True)
                )

            self.transform()
            self.write()
            self.input_data_objects[SilverTimeSegmentsDataObject.ID] = self.output_data_objects[
                SilverTimeSegmentsDataObject.ID
            ]
            self.is_first_run = False

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        current_events_sdf = self.current_input_events_sdf
        last_time_segments_sdf = self.last_time_segments
        intersections_groups_df = self.current_interesection_groups_sdf

        # Add overlapping_cell_ids list to each event
        current_events_sdf = (
            current_events_sdf.alias("df1")
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
                f"df1.{ColNames.user_id_modulo}",
                ColNames.overlapping_cell_ids,
            )
        )

        if last_time_segments_sdf is None:
            current_events_sdf = (
                current_events_sdf.withColumn(ColNames.end_timestamp, F.lit(None).cast(TimestampType()))
                .withColumn(ColNames.cells, F.lit(None))
                .withColumn(ColNames.state, F.lit(None))
                .withColumn("segment_plmn", F.lit(None))
            )
        else:
            last_time_segments_sdf = last_time_segments_sdf.select(
                ColNames.end_timestamp,
                ColNames.cells,
                ColNames.state,
                ColNames.user_id,
                F.col(ColNames.mcc).alias("segment_mcc"),
                F.col(ColNames.mnc).alias("segment_mnc"),
                F.col(ColNames.plmn).alias("segment_plmn"),
                ColNames.user_id_modulo,
            )
            current_events_sdf = current_events_sdf.join(
                F.broadcast(last_time_segments_sdf),
                on=[ColNames.user_id_modulo, ColNames.user_id],
                how="outer",
            )

            current_events_sdf = (
                current_events_sdf.withColumn(
                    ColNames.mcc, F.coalesce(F.col(ColNames.mcc), F.col("segment_mcc"))
                ).withColumn(ColNames.mnc, F.coalesce(F.col(ColNames.mnc), F.col("segment_mnc")))
            ).drop("segment_mcc", "segment_mnc")

        # TODO add first event(s?) from next date to current events to handle last segment of date

        # TODO: This conversion is needed for Pandas serialisation/deserialisation,
        # to remove it when user_id will be stored as string, not as binary
        current_events_sdf = current_events_sdf.withColumn(ColNames.user_id, F.hex(F.col(ColNames.user_id)))

        current_events_sdf = current_events_sdf.withColumn(
            "is_abroad_event",
            (F.col(ColNames.plmn).isNotNull()) & (F.col(ColNames.plmn).substr(1, 3) != F.lit(self.local_mcc)),
        )

        # Partial function to pass the current date and other parameters to the aggregation function
        aggregate_segments_partial = partial(
            self.aggregate_segments,
            current_date=self.current_date,
            min_time_stay=self.min_time_stay,
            max_time_missing_stay=self.max_time_missing_stay,
            max_time_missing_move=self.max_time_missing_move,
            max_time_missing_abroad=self.max_time_missing_abroad,
            pad_time=self.pad_time,
        )

        # TODO: To test this approach with large datasets, might not be feasible
        current_segments_sdf = current_events_sdf.groupby(ColNames.user_id_modulo, ColNames.user_id).applyInPandas(
            aggregate_segments_partial, self.segmentation_return_schema
        )

        current_segments_sdf = current_segments_sdf.withColumns(
            {
                ColNames.year: F.year(ColNames.start_timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.start_timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.start_timestamp).cast("tinyint"),
            }
        )

        # TODO: This conversion is needed to get back to binary after Pandas serialisation/deserialisation,
        # to remove it when user_id will be stored as string, not as binary
        current_segments_sdf = current_segments_sdf.withColumn(ColNames.user_id, F.unhex(F.col(ColNames.user_id)))

        current_segments_sdf = apply_schema_casting(current_segments_sdf, SilverTimeSegmentsDataObject.SCHEMA)
        current_segments_sdf = current_segments_sdf.repartition(
            *SilverTimeSegmentsDataObject.PARTITION_COLUMNS
        ).sortWithinPartitions(ColNames.user_id, ColNames.start_timestamp)

        self.output_data_objects[SilverTimeSegmentsDataObject.ID].df = current_segments_sdf

    @staticmethod
    def aggregate_segments(
        pdf: pd.DataFrame,
        current_date: date,
        min_time_stay: timedelta,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        max_time_missing_abroad: timedelta,
        pad_time: timedelta,
    ) -> pd.DataFrame:
        """Aggregates user stays into continuous time segments based on given parameters.
        This function processes user location data and creates continuous time segments,
        taking into account various time-based parameters to determine segment boundaries and types.
        Args:
            pdf: DataFrame containing user location events.
            current_date: Date for which to generate segments.
            min_time_stay: Minimum duration required to consider a period as a stay.
            max_time_missing_stay: Maximum allowed gap in data while maintaining a stay segment.
            max_time_missing_move: Maximum allowed gap in data while maintaining a move segment.
            max_time_missing_abroad: Maximum allowed gap in data for abroad segments.
            pad_time: Time padding to add around segments.
        Returns:
            DataFrame containing aggregated time segments.
        """
        user_id, user_mod, mcc, mnc = ContinuousTimeSegmentation._get_user_metadata(pdf)

        # Prepare date boundaries
        current_date_start = datetime.combine(current_date, time(0, 0, 0))
        current_date_end = datetime.combine(current_date, time(23, 59, 59))

        # Check if there are any events for this date
        no_events_for_current_date = pdf[ColNames.timestamp].isna().all()
        no_previous_segments = pdf[ColNames.end_timestamp].isna().all()

        if no_events_for_current_date:
            # If no events, create a single UNKNOWN segment
            segments = ContinuousTimeSegmentation._handle_no_events_for_current_date(
                pdf, no_previous_segments, user_id, current_date_start, current_date_end, max_time_missing_abroad
            )
        else:
            # Create the initial time segment for this day
            current_ts = ContinuousTimeSegmentation._create_initial_time_segment(
                pdf,
                no_previous_segments,
                current_date_start,
                pad_time,
                user_id,
                max_time_missing_stay,
                max_time_missing_move,
                max_time_missing_abroad,
            )

            # Limit columns we actually need
            pdf_for_events = pdf[
                [ColNames.timestamp, ColNames.cell_id, ColNames.overlapping_cell_ids, "is_abroad_event", ColNames.plmn]
            ]

            # Build segments from each event
            segments = ContinuousTimeSegmentation._iterate_events(
                pdf_for_events,
                current_ts,
                user_id,
                min_time_stay,
                max_time_missing_stay,
                max_time_missing_move,
                max_time_missing_abroad,
                pad_time,
            )

        # Convert list of segments to DataFrame
        segments_df = pd.DataFrame(segments)
        segments_df[ColNames.user_id] = user_id
        segments_df[ColNames.mcc] = mcc
        segments_df[ColNames.mnc] = mnc
        segments_df[ColNames.user_id_modulo] = user_mod

        return segments_df

    # ---------------------  No-Events Helper  ---------------------
    @staticmethod
    def _handle_no_events_for_current_date(
        pdf: pd.DataFrame,
        no_previous_segments: bool,
        user_id: str,
        day_start: datetime,
        day_end: datetime,
        max_time_missing_abroad: timedelta,
    ) -> List[Dict]:
        """Handles cases where there are no events for the current date.
        This method creates a time segment for a day without events. If there were previous segments
        and the last segment was abroad within the maximum allowed time gap, it continues the abroad state.
        Otherwise, it creates an unknown state segment.
        Args:
            pdf (pd.DataFrame): DataFrame containing previous segments information
            no_previous_segments (bool): Flag indicating if there are previous segments
            user_id (str): Identifier for the user
            day_start (datetime): Start timestamp of the day
            day_end (datetime): End timestamp of the day
            max_time_missing_abroad (timedelta): Maximum allowed time gap for continuing abroad state
        Returns:
            List[Dict]: List containing a single time segment dictionary with the appropriate state
        """
        if not no_previous_segments:
            previous_segment_end_time = pdf[ColNames.end_timestamp].iloc[0]
            previous_segment_state = pdf[ColNames.state].iloc[0]
            previous_segment_plmn = pdf["segment_plmn"].iloc[0]

            if (previous_segment_state == SegmentStates.ABROAD) and (
                day_end - previous_segment_end_time <= max_time_missing_abroad
            ):
                seg = ContinuousTimeSegmentation._create_time_segment(
                    day_start, day_end, [], previous_segment_plmn, SegmentStates.ABROAD, user_id
                )
            else:
                seg = ContinuousTimeSegmentation._create_time_segment(
                    day_start, day_end, [], None, SegmentStates.UNKNOWN, user_id
                )
        else:
            seg = ContinuousTimeSegmentation._create_time_segment(
                day_start, day_end, [], None, SegmentStates.UNKNOWN, user_id
            )

        seg[ColNames.is_last] = True
        return [seg]

    # ---------------------  Initial Segment Helper  ---------------------
    @staticmethod
    def _create_initial_time_segment(
        pdf: pd.DataFrame,
        no_previous_segments: bool,
        day_start: datetime,
        pad_time: timedelta,
        user_id: str,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        max_time_missing_abroad: timedelta,
    ) -> Dict:
        """Create initial time segment based on first event and previous day information.
        Creates a time segment from the start of the day until the first event of the day,
        considering any existing segments from the previous day to maintain continuity.
        Args:
            pdf: DataFrame containing the first event data
            no_previous_segments: Boolean indicating if there are segments from previous day
            day_start: DateTime marking the start of the current day
            pad_time: TimeDelta for padding unknown segments
            user_id: String identifier for the user
            max_time_missing_stay: Maximum allowed gap for stay segments
            max_time_missing_move: Maximum allowed gap for move segments
            max_time_missing_abroad: Maximum allowed gap for abroad segments
        Returns:
            Dict containing the created time segment
        """
        first_event_time = pdf[ColNames.timestamp].iloc[0]
        previous_segment_end_time = pdf[ColNames.end_timestamp].iloc[0]
        previous_segment_state = pdf[ColNames.state].iloc[0]
        previous_segment_plmn = pdf["segment_plmn"].iloc[0]
        previous_segment_cells = pdf[ColNames.cells].iloc[0]

        time_to_first_event = first_event_time - day_start
        adjusted_pad = min(pad_time, time_to_first_event / 2)

        if no_previous_segments:
            # No segment from previous day => unknown until first event
            return ContinuousTimeSegmentation._create_time_segment(
                day_start,
                first_event_time - adjusted_pad,
                [],
                None,
                SegmentStates.UNKNOWN,
                user_id,
            )

        # Otherwise, try to continue from the previous day
        gap = first_event_time - previous_segment_end_time

        if (previous_segment_state == SegmentStates.STAY) and (gap <= max_time_missing_stay):
            return ContinuousTimeSegmentation._create_time_segment(
                day_start,
                first_event_time,
                previous_segment_cells,
                previous_segment_plmn,
                SegmentStates.STAY,
                user_id,
            )
        elif (previous_segment_state == SegmentStates.MOVE) and (gap <= max_time_missing_move):
            return ContinuousTimeSegmentation._create_time_segment(
                day_start,
                first_event_time,
                previous_segment_cells,
                previous_segment_plmn,
                SegmentStates.MOVE,
                user_id,
            )
        elif (previous_segment_state == SegmentStates.ABROAD) and (gap <= max_time_missing_abroad):
            return ContinuousTimeSegmentation._create_time_segment(
                day_start,
                first_event_time,
                [],
                previous_segment_plmn,
                SegmentStates.ABROAD,
                user_id,
            )
        else:
            # Large gap or incompatible => unknown until first event
            return ContinuousTimeSegmentation._create_time_segment(
                day_start,
                first_event_time - adjusted_pad,
                [],
                None,
                SegmentStates.UNKNOWN,
                user_id,
            )

    # ---------------------  Iteration Over Events ---------------------
    @staticmethod
    def _iterate_events(
        pdf_events: pd.DataFrame,
        current_ts: Dict,
        user_id: str,
        min_time_stay: timedelta,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        max_time_missing_abroad: timedelta,
        pad_time: timedelta,
    ) -> List[Dict]:
        """Iterates through events and constructs time segments based on continuous time segmentation rules.

        Processes a sequence of events (both abroad and local) and creates time segments according to
        specified time constraints. Each event updates the current time segment state and may generate
        new segments when conditions are met.

        Args:
            pdf_events: DataFrame containing events with timestamp, location, and other relevant information.
            current_ts: Dictionary representing the current time segment state.
            user_id: String identifier for the user.
            min_time_stay: Minimum duration required for a stay segment.
            max_time_missing_stay: Maximum allowed gap in stay segments.
            max_time_missing_move: Maximum allowed gap in movement segments.
            max_time_missing_abroad: Maximum allowed gap in abroad segments.
            pad_time: Time padding added to segments.

        Returns:
            List of dictionaries representing time segments
        """
        all_segments: List[Dict] = []

        for event in pdf_events.itertuples(index=False):
            if event.is_abroad_event:
                # Process abroad logic
                new_segments, new_current = ContinuousTimeSegmentation._process_abroad_event(
                    current_ts,
                    user_id,
                    event.timestamp,
                    event.plmn,
                    max_time_missing_abroad,
                )
            else:
                # Process local logic
                new_segments, new_current = ContinuousTimeSegmentation._process_local_event(
                    current_ts,
                    user_id,
                    event.timestamp,
                    event.cell_id,
                    event.overlapping_cell_ids,
                    event.plmn,
                    min_time_stay,
                    max_time_missing_stay,
                    max_time_missing_move,
                    pad_time,
                )

            all_segments.extend(new_segments)
            current_ts = new_current

        # Mark final segment as is_last
        current_ts[ColNames.is_last] = True
        all_segments.append(current_ts)
        return all_segments

    @staticmethod
    def _extend_segment(current_ts: Dict, new_end_time: datetime, new_cells: List[Any] = None) -> Dict:
        """
        Returns a brand new segment dictionary with an extended_ts end_time
        and optionally merged cells. Does not mutate the original.
        """
        updated_ts = current_ts.copy()
        updated_ts[ColNames.end_timestamp] = new_end_time

        if new_cells is not None:
            merged_cells = list(set(updated_ts[ColNames.cells] + new_cells))
            updated_ts[ColNames.cells] = merged_cells

        return updated_ts

    # ---------------------  Processing Each Event ---------------------
    @staticmethod
    def _process_abroad_event(
        current_ts: Dict,
        user_id: str,
        event_timestamp: datetime,
        event_plmn: str,
        max_time_missing_abroad: timedelta,
    ) -> Tuple[List[Dict], Dict]:
        """
        Decide whether to extend current ABROAD segment, create a new one,
        or start bridging with UNKNOWN if the gap is too large.
        Returns (finalized_segments, new_current_ts).
        """
        segments_to_add: List[Dict] = []

        abroad_mcc = str(event_plmn)[:3]
        current_mcc = str(current_ts.get(ColNames.plmn) or "")[:3]
        is_mcc_matched = abroad_mcc == current_mcc

        gap = event_timestamp - current_ts[ColNames.end_timestamp]

        if current_ts[ColNames.state] != SegmentStates.ABROAD:
            # Transition from a different state to ABROAD
            segments_to_add.append(current_ts)
            current_ts = ContinuousTimeSegmentation._create_time_segment(
                current_ts[ColNames.end_timestamp],
                event_timestamp,
                [],
                event_plmn,
                SegmentStates.ABROAD,
                user_id,
            )

        elif is_mcc_matched and (gap <= max_time_missing_abroad):
            # Extend existing ABROAD
            current_ts = ContinuousTimeSegmentation._extend_segment(current_ts, event_timestamp)

        elif (not is_mcc_matched) and (gap <= max_time_missing_abroad):
            # Different MCC but within the gap => new ABROAD segment
            segments_to_add.append(current_ts)
            current_ts = ContinuousTimeSegmentation._create_time_segment(
                current_ts[ColNames.end_timestamp],
                event_timestamp,
                [],
                event_plmn,
                SegmentStates.ABROAD,
                user_id,
            )

        else:
            # Gap too large => bridging with UNKNOWN
            segments_to_add.append(current_ts)
            current_ts = ContinuousTimeSegmentation._create_time_segment(
                current_ts[ColNames.end_timestamp],
                event_timestamp,
                [],
                event_plmn,
                SegmentStates.UNKNOWN,
                user_id,
            )

        return segments_to_add, current_ts

    @staticmethod
    def _process_local_event(
        current_ts: Dict,
        user_id: str,
        event_timestamp: datetime,
        event_cell: Any,
        overlapping_cell_ids: Any,
        event_plmn: Any,
        min_time_stay: timedelta,
        max_time_missing_stay: timedelta,
        max_time_missing_move: timedelta,
        pad_time: timedelta,
    ) -> Tuple[List[Dict], Dict]:
        """
        Decide whether to continue a STAY/UNDETERMINED, transition to MOVE,
        or insert UNKNOWN bridging based on the local event.
        Returns (finalized_segments, updated_current_ts).
        """
        segments_to_add: List[Dict] = []
        gap = event_timestamp - current_ts[ColNames.end_timestamp]
        if overlapping_cell_ids is None:
            overlapping_cell_ids = []
        new_cells = overlapping_cell_ids.tolist()
        new_cells.append(event_cell)

        is_intersected = ContinuousTimeSegmentation._check_intersection(
            current_ts[ColNames.cells],
            new_cells,
        )

        # Case 1: UNKNOWN/ABROAD => UNDETERMINED transition
        if current_ts[ColNames.state] in [SegmentStates.UNKNOWN, SegmentStates.ABROAD]:
            segments_to_add.append(current_ts)
            current_ts = ContinuousTimeSegmentation._create_time_segment(
                current_ts[ColNames.end_timestamp],
                event_timestamp,
                [event_cell],
                event_plmn,
                SegmentStates.UNDETERMINED,
                user_id,
            )

        # Case 2: Intersection => STAY or UNDETERMINED extension
        elif is_intersected and (gap <= max_time_missing_stay):
            if current_ts[ColNames.state] in [SegmentStates.UNDETERMINED, SegmentStates.STAY]:
                # Extend in place
                current_ts = ContinuousTimeSegmentation._extend_segment(current_ts, event_timestamp, [event_cell])
                duration = current_ts[ColNames.end_timestamp] - current_ts[ColNames.start_timestamp]
                if duration > min_time_stay:
                    current_ts[ColNames.state] = SegmentStates.STAY

            elif current_ts[ColNames.state] == SegmentStates.MOVE:
                # End MOVE => start UNDETERMINED
                segments_to_add.append(current_ts)
                current_ts = ContinuousTimeSegmentation._create_time_segment(
                    current_ts[ColNames.end_timestamp],
                    event_timestamp,
                    [event_cell],
                    event_plmn,
                    SegmentStates.UNDETERMINED,
                    user_id,
                )
        # Case 3: No intersection but gap <= max_time_missing_move => 'move'
        elif (not is_intersected) and (gap <= max_time_missing_move):

            midpoint = current_ts[ColNames.end_timestamp] + gap / 2
            move_ts_1 = ContinuousTimeSegmentation._create_time_segment(
                current_ts[ColNames.end_timestamp],
                midpoint,
                current_ts[ColNames.cells],
                event_plmn,
                SegmentStates.MOVE,
                user_id,
            )
            segments_to_add.extend([current_ts, move_ts_1])

            current_ts = ContinuousTimeSegmentation._create_time_segment(
                midpoint,
                event_timestamp,
                [event_cell],
                event_plmn,
                SegmentStates.MOVE,
                user_id,
            )

        # Case 4: Gap too large => bridging with UNKNOWN
        else:
            # First, artificially extend current_ts by pad_time
            extended_ts = ContinuousTimeSegmentation._extend_segment(
                current_ts, current_ts[ColNames.end_timestamp] + pad_time
            )

            unknown_segment = ContinuousTimeSegmentation._create_time_segment(
                extended_ts[ColNames.end_timestamp],
                event_timestamp - pad_time,
                [],
                event_plmn,
                SegmentStates.UNKNOWN,
                user_id,
            )

            segments_to_add.extend([extended_ts, unknown_segment])

            current_ts = ContinuousTimeSegmentation._create_time_segment(
                event_timestamp - pad_time,
                event_timestamp,
                [event_cell],
                event_plmn,
                SegmentStates.UNDETERMINED,
                user_id,
            )

        return segments_to_add, current_ts

    @staticmethod
    def _create_time_segment(
        start_timestamp: datetime,
        end_timestamp: datetime,
        cells: List[str],
        plmn: int,
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
            ColNames.plmn: plmn,
            ColNames.state: state,
            ColNames.is_last: False,
        }

    @staticmethod
    def _get_user_metadata(pdf: pdDataFrame) -> Tuple[str, int, str]:
        """
        Gets user_id, user_id_modulo, mcc, mnc from Pandas DataFrame containing columns with the corresponding names.
        Values from the first row of the dataframe are used.

        Args:
            pdf (pdDataFrame): Pandas DataFrame

        Returns:
            Tuple[str, int, str]: user_id, user_id_modulo, mcc, mnc
        """
        user_id = pdf[ColNames.user_id][0]
        user_id_mod = pdf[ColNames.user_id_modulo][0]
        mcc = pdf[ColNames.mcc][0]
        mnc = pdf[ColNames.mnc][0]
        return user_id, user_id_mod, mcc, mnc

    @staticmethod
    def _check_intersection(
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
