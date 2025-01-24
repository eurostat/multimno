import pytest
import hashlib
from configparser import ConfigParser
from datetime import datetime
from multimno.core.constants.columns import ColNames
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
    cell_intersection_groups_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        event_data (list[Row]): list of event data rows
        cell_intersection_groups_data (list[Row]): list of cell intersection groups data rows
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]

    ### Write input event data to test resources dir
    event_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    input_events_do = SilverEventFlaggedDataObject(spark, event_data_path)
    input_events_do.df = spark.createDataFrame(event_data, schema=SilverEventFlaggedDataObject.SCHEMA).orderBy(
        ColNames.user_id, ColNames.timestamp
    )
    input_events_do.write(partition_columns=partition_columns)

    ### Write input cell intersection groups data to test resources dir
    cell_intersection_groups_data_path = config["Paths.Silver"]["cell_intersection_groups_data_silver"]
    input_cell_intersection_groups_do = SilverCellIntersectionGroupsDataObject(
        spark, cell_intersection_groups_data_path
    )
    input_cell_intersection_groups_do.df = spark.createDataFrame(
        cell_intersection_groups_data,
        schema=SilverCellIntersectionGroupsDataObject.SCHEMA,
    )
    input_cell_intersection_groups_do.write(partition_columns=partition_columns)


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
        Row(  # User is is at cell_id_a
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T03:03:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
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
        Row(  # User has moved to cell_id_a with enough time in between to cause "unknown" state
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T07:17:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
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
        Row(  # User is at cell_id_a, but not enough time is spent to cause a stay, so state is "undetermined"
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
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
        Row(  # Day 4 (next day). User is at cell_id_b1, but time gap is above stay max.
            user_id=user_id,
            timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
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
    # Expected output: time segments.
    # One time segment of type stay on the day data was present.
    # No time segments before that date.
    # Entire day time segments of type unknown for dates after with no data.
    unhex_user_id = user_id.hex().upper()

    expected_output_data = [
        Row(  # "unknown" segment for entire date before first events
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-02T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-02T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-02T23:59:59", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=True,
            year=year,
            month=month,
            day=day - 1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # "unknown" segment from start of day until first event
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="stay",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="move",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state="move",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_b1,cell_id_b2
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T03:44:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T03:44:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T04:45:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1, cell_id_b2],
            state="stay",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Undetermined section
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:12:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:12:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:18:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="undetermined",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="move",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move section to cell_id_b1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-03T07:19:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-03T07:19:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T07:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state="move",
            is_last=True,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Day 4. "unknown" section
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:00:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:15:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=False,
            year=year,
            month=month,
            day=4,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Undetermined at cell_id_b1
            user_id=user_id,
            time_segment_id=hashlib.md5(
                f"{unhex_user_id}{datetime.strptime('2023-01-04T00:15:00', date_format)}".encode()
            ).hexdigest(),
            start_timestamp=datetime.strptime("2023-01-04T00:15:00", date_format),
            end_timestamp=datetime.strptime("2023-01-04T00:20:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state="undetermined",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state="move",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="move",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="move",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1],
            state="move",
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
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=True,
            year=year,
            month=month,
            day=5,
            user_id_modulo=user_id_modulo,
        ),
    ]

    return {
        input_events_id: input_event_data,
        input_cell_intersection_groups_id: cell_intersection_groups_data,
        expected_output_time_segments_id: expected_output_data,
    }
