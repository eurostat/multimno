import pytest
import hashlib
from configparser import ConfigParser
from datetime import datetime
from multimno.core.constants.columns import ColNames, SegmentStates
from multimno.core.constants.domain_names import Domains
from multimno.core.data_objects.silver.event_cache_data_object import EventCacheDataObject
from multimno.core.spark_session import delete_file_or_folder
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row

from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.data_objects.silver.silver_time_segments_data_object import (
    SilverTimeSegmentsDataObject,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]

input_events_id = "input_events"
input_cell_intersection_groups_id = "input_cell_intersection_groups"
input_event_cache_id = "input_event_cache_data"
input_time_segments_id = "input_time_segments"
expected_output_time_segments_id = "expected_time_segments"


def get_expected_output_df(spark: SparkSession, expected_time_segments_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTimeSegmentsDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_time_segments_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_time_segments_data, schema=SilverTimeSegmentsDataObject.SCHEMA)
    return expected_data_df


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    event_data: list[Row],
    event_cache_data: list[Row],
    input_time_segments_data: list[Row],
    cell_intersection_groups_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        event_data (list[Row]): list of event data rows
        event_cache_data (list[Row]): list of event cache data rows
        input_time_segments_data (list[Row]): list of existing time segment data rows
        cell_intersection_groups_data (list[Row]): list of cell intersection groups data rows
    """
    ### Write input event data to test resources dir
    event_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    delete_file_or_folder(spark, event_data_path)
    input_events_do = SilverEventFlaggedDataObject(spark, event_data_path)
    input_events_do.df = spark.createDataFrame(event_data, schema=SilverEventFlaggedDataObject.SCHEMA).orderBy(
        ColNames.user_id, ColNames.timestamp
    )
    input_events_do.write(partition_columns=SilverEventFlaggedDataObject.PARTITION_COLUMNS)

    ### Write input event cache data to test resources dir
    event_cache_data_path = config["Paths.Silver"]["event_cache"]
    delete_file_or_folder(spark, event_cache_data_path)
    input_event_cache_do = EventCacheDataObject(spark, event_cache_data_path)
    input_event_cache_do.df = spark.createDataFrame(
        event_cache_data,
        schema=EventCacheDataObject.SCHEMA,
    )
    input_event_cache_do.write(partition_columns=EventCacheDataObject.PARTITION_COLUMNS)

    ### Write input time segments data to test resources dir
    time_segments_data_path = config["Paths.Silver"]["time_segments_silver"]
    delete_file_or_folder(spark, time_segments_data_path)
    input_time_segments_do = SilverTimeSegmentsDataObject(spark, time_segments_data_path)
    input_time_segments_do.df = spark.createDataFrame(
        input_time_segments_data,
        schema=SilverTimeSegmentsDataObject.SCHEMA,
    )
    input_time_segments_do.write(partition_columns=SilverTimeSegmentsDataObject.PARTITION_COLUMNS)

    ### Write input cell intersection groups data to test resources dir
    cell_intersection_groups_data_path = config["Paths.Silver"]["cell_intersection_groups_data_silver"]
    delete_file_or_folder(spark, cell_intersection_groups_data_path)
    input_cell_intersection_groups_do = SilverCellIntersectionGroupsDataObject(
        spark, cell_intersection_groups_data_path
    )
    input_cell_intersection_groups_do.df = spark.createDataFrame(
        cell_intersection_groups_data,
        schema=SilverCellIntersectionGroupsDataObject.SCHEMA,
    )
    input_cell_intersection_groups_do.write(partition_columns=SilverCellIntersectionGroupsDataObject.PARTITION_COLUMNS)


def generate_event_cache_data(input_event_data: list[Row]) -> list[Row]:
    """
    Generates EventCacheDataObject data from input event data, selecting first and last event per date per user and adding "is_last_event" column.
    """

    # Collect first and last event per date per user
    first_last_event_dict = {}  # (user_id, date, is_last_event) : Row
    for row in input_event_data:

        user_id = row.user_id
        date = datetime(year=row.year, month=row.month, day=row.day)
        first_key = (user_id, date, False)
        last_key = (user_id, date, True)
        row_dict = row.asDict()

        if (first_key not in first_last_event_dict) or (first_last_event_dict[first_key].timestamp > row.timestamp):
            row_dict[ColNames.is_last_event] = False
            first_last_event_dict[first_key] = Row(**row_dict)

        if (last_key not in first_last_event_dict) or (first_last_event_dict[last_key].timestamp < row.timestamp):
            row_dict[ColNames.is_last_event] = True
            first_last_event_dict[last_key] = Row(**row_dict)

    event_cache_data = first_last_event_dict.values()

    return event_cache_data


def data_test_0001() -> dict:
    # Test case: one user. Combination of stay, move, undetermined, unknown segments.
    date_format = "%Y-%m-%dT%H:%M:%S"
    cell_id_a = "a0001"
    cell_id_b1 = "b0001"
    cell_id_b2 = "b0002"
    user_id = hashlib.sha256("1000".encode()).digest()
    mcc = 100
    mnc = "01"
    plmn = None
    domain = Domains.DOMESTIC
    year = 2023
    month = 1
    day = 3
    user_id_modulo = 0
    # Input: event data. One user's events, nearby in time, same cell.
    input_event_data = [
        Row(  # First event, user is at cell_id_a
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T01:00:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T01:01:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T02:02:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User is at cell_id_a
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User has moved to cell_id_b1
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T03:44:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b1,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User is at cell_id_b2, which is within overlap of cell_id_b1
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T03:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b2,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User is still in overlap of cell_id_b1
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T04:40:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b1,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User has moved to cell_id_a with enough time in between to cause SegmentStates.UNKNOWN state
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T07:17:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User is at cell_id_a, but not enough time is spent to cause a stay, so state is SegmentStates.UNDETERMINED
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User has quickly moved to cell_id_b1
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T07:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b1,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 4 (next day). User is still at cell_id_b1, but time gap is long.
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b1,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User has quickly moved to cell_id_a.
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-04T00:25:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # User has quickly moved back to cell_id_b1.
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-04T00:27:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_b1,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Outbound event
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-06T17:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=12345,
            domain=Domains.OUTBOUND,
            cell_id=None,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Outbound event
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-06T00:27:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=12345,
            domain=Domains.OUTBOUND,
            cell_id=None,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=year,
            month=month,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
    ]
    # Input: cell intersection groups data.
    # Case: single cell in one group. Cell id and day matches event data.
    cell_intersection_groups_data = [
        # Day day overlaps
        Row(
            cell_id=cell_id_a,
            overlapping_cell_ids=[],
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_b1,
            overlapping_cell_ids=[cell_id_b2],
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_b2,
            overlapping_cell_ids=[cell_id_b1],
            year=year,
            month=month,
            day=day,
        ),
        # Day 4 overlaps
        Row(
            cell_id=cell_id_a,
            overlapping_cell_ids=[],
            year=year,
            month=month,
            day=day + 1,
        ),
        Row(
            cell_id=cell_id_b1,
            overlapping_cell_ids=[cell_id_b2],
            year=year,
            month=month,
            day=day + 1,
        ),
        Row(
            cell_id=cell_id_b2,
            overlapping_cell_ids=[cell_id_b1],
            year=year,
            month=month,
            day=day + 1,
        ),
    ]

    # Input: event cache data.
    # Input events containing only the first and last event per date per user.
    input_event_cache_data = generate_event_cache_data(input_event_data)

    # Input: existing time segments.
    # This test does not have any values here.
    input_time_segments_data = []

    # Expected output: time segments.
    # One time segment of type stay on the day data was present.
    # No time segments before that date.
    # Entire day time segments of type unknown for dates after with no data.
    unhex_user_id = user_id.hex().upper()

    expected_output_data = [
        Row(  # Day 3 starts. SegmentStates.UNKNOWN segment from start of day until first event
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T00:55:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.STAY,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move half from cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T03:03:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T03:23:30", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move half to cell_id_b1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T03:23:30', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T03:23:30", date_format),
            end_timestamp=datetime.strptime("2023-01-03T03:44:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T03:44:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_b1,cell_id_b2. End timestamp is extended due to the following UNKNOWN segment.
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T03:44:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T03:44:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T04:45:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T04:40:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1, cell_id_b2],
            state=SegmentStates.STAY,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Unknown section
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T04:45:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T04:45:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:12:00", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Undetermined section, approaching cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:12:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:12:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.UNDETERMINED,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move section from cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:18:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:19:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move section to cell_id_b1. Next event is in next date and far, so end is extended.
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:19:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:19:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:25:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-03T07:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # UNKNOWN segment across midnight, first half.
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:25:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:25:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=None,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 4. UNKNOWN segment across midnight, second half.
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:15:00", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # UNDETERMINED segment after long UNKNOWN.
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:15:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:15:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state=SegmentStates.UNDETERMINED,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # First half of move to cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:20:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:22:30", date_format),
            last_event_timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Second half of move to cell_id_a
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:22:30', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:22:30", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:25:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-04T00:25:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # First half of move to cell_id_b1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:25:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:25:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:26:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-04T00:25:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Second half of move to cell_id_b1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:26:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:26:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:27:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-04T00:27:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state=SegmentStates.MOVE,
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Rest of the day without events + next day has no events -> UNKNOWN until end of date
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:27:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:27:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=None,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 5. no events
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-05T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-05T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-05T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=None,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=year,
            month=month,
            day=5,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 6. First segment is unknown
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-06T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-06T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-06T00:22:00", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=False,
            year=year,
            month=month,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 6. Abroad
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-06T00:22:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-06T00:22:00", date_format),
            end_timestamp=datetime.strptime("2023-01-06T17:55:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-06T17:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=12345,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=year,
            month=month,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Rest of the day without events + next day has no events -> UNKNOWN until end of date
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-06T17:55:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-06T17:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-06T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=None,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=year,
            month=month,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
    ]

    return {
        input_events_id: input_event_data,
        input_cell_intersection_groups_id: cell_intersection_groups_data,
        input_event_cache_id: input_event_cache_data,
        input_time_segments_id: input_time_segments_data,
        expected_output_time_segments_id: expected_output_data,
    }


def data_test_0002() -> dict:
    # Has initial existing time segments data.
    date_format = "%Y-%m-%dT%H:%M:%S"
    cell_id_a = "a0001"
    cell_id_b1 = "b0001"
    cell_id_b2 = "b0002"
    user_id = hashlib.sha256("1000".encode()).digest()
    unhex_user_id = user_id.hex().upper()
    mcc = 100
    mnc = "01"
    plmn = None
    domain = Domains.DOMESTIC
    user_id_modulo = 0

    # Input: event data. One user's events.
    input_event_data = [
        Row(  # First event, user is at cell_id_a
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-01T01:00:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            domain=domain,
            cell_id=cell_id_a,
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
    ]
    # Input: cell intersection groups data.
    # Case: single cell in one group. Cell id and day matches event data.
    cell_intersection_groups_data = [
        # Day day overlaps
        Row(
            cell_id=cell_id_a,
            overlapping_cell_ids=[],
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id=cell_id_b1,
            overlapping_cell_ids=[cell_id_b2],
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id=cell_id_b2,
            overlapping_cell_ids=[cell_id_b1],
            year=2023,
            month=1,
            day=3,
        ),
        # Day 4 overlaps
        Row(
            cell_id=cell_id_a,
            overlapping_cell_ids=[],
            year=2023,
            month=1,
            day=4,
        ),
        Row(
            cell_id=cell_id_b1,
            overlapping_cell_ids=[cell_id_b2],
            year=2023,
            month=1,
            day=4,
        ),
        Row(
            cell_id=cell_id_b2,
            overlapping_cell_ids=[cell_id_b1],
            year=2023,
            month=1,
            day=4,
        ),
    ]

    # Input: event cache data.
    # Input events containing only the first and last event per date per user.
    input_event_cache_data = generate_event_cache_data(input_event_data)

    # Input: existing time segments.
    input_time_segments_data = [
        Row(
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2022-12-31T05:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2022-12-31T05:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-31T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2022,
            month=12,
            day=31,
            user_id_modulo=user_id_modulo,
        ),
    ]

    # Expected output: time segments.
    expected_output_data = [
        Row(  # Day -1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2022-12-31T05:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2022-12-31T05:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-31T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2022,
            month=12,
            day=31,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 1 UNKNOWN
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-01T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-01T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T00:55:00", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 1 UNDETERMINED
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-01T00:55:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-01T00:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T01:00:00", date_format),
            last_event_timestamp=datetime.strptime("2023-01-01T01:00:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state=SegmentStates.UNDETERMINED,
            is_last=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 1 UNKNOWN
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-01T01:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-01T01:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 2
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-02T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-02T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-02T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=2,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 3
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=3,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 4
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 5
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-05T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-05T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-05T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=5,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 6
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-06T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-06T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-06T23:59:59", date_format),
            last_event_timestamp=None,
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state=SegmentStates.UNKNOWN,
            is_last=True,
            year=2023,
            month=1,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
    ]

    return {
        input_events_id: input_event_data,
        input_cell_intersection_groups_id: cell_intersection_groups_data,
        input_time_segments_id: input_time_segments_data,
        input_event_cache_id: input_event_cache_data,
        expected_output_time_segments_id: expected_output_data,
    }
