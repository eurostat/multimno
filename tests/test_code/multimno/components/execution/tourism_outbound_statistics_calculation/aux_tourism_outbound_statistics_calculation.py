from configparser import ConfigParser
from datetime import date, datetime
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.constants.columns import ColNames, SegmentStates

from multimno.core.data_objects.bronze.bronze_mcc_iso_tz_map import BronzeMccIsoTzMap
from multimno.core.data_objects.silver.silver_time_segments_data_object import SilverTimeSegmentsDataObject
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from tests.test_code.fixtures import spark_session as spark


input_time_segments_id = "time_segments_silver"
input_tourism_trips_id = "tourism_outbound_trips_silver"
mcc_iso_timezones_id = "mcc_iso_timezones_data_bronze"

expected_tourism_trips_id = "expected_tourism_trips_silver"
expected_tourism_outbound_nights_spent_id = "expected_tourism_outbound_nights_spent_silver"

mcc_iso_timezones_input_path = (
    "/opt/app/tests/test_resources/test_data/tourism_outbound_statistics_calculation/mcc_iso_timezones.parquet"
)


def get_expected_trips_output_df(spark: SparkSession, expected_trips_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismTripDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_trips_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_trips_data, schema=SilverTourismTripDataObject.SCHEMA)
    return expected_data_df


def get_expected_agg_output_df(spark: SparkSession, expected_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismOutboundNightsSpentDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_data, schema=SilverTourismOutboundNightsSpentDataObject.SCHEMA)
    return expected_data_df


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    time_segments_data: list[Row],
    tourism_trips_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        tourism_stays_data (list[Row]): list of tourism stays data rows
        tourism_trips_data (list[Row]): list of tourism trips data rows
    """

    ### Write input tourism stays data to test resources dir
    time_segments_data_path = config["Paths.Silver"][input_time_segments_id]
    input_time_segments_do = SilverTimeSegmentsDataObject(spark, time_segments_data_path)
    input_time_segments_do.df = spark.createDataFrame(
        time_segments_data, schema=SilverTimeSegmentsDataObject.SCHEMA
    ).orderBy(ColNames.user_id, ColNames.start_timestamp)
    partition_columns = SilverTimeSegmentsDataObject.PARTITION_COLUMNS
    input_time_segments_do.write(partition_columns=partition_columns)

    ### Write input tourism trips data to test resources dir
    tourism_trips_data_path = config["Paths.Silver"][input_tourism_trips_id]
    tourism_trips_do = SilverTourismTripDataObject(spark, tourism_trips_data_path)
    tourism_trips_do.df = spark.createDataFrame(tourism_trips_data, schema=SilverTourismTripDataObject.SCHEMA)
    partition_columns = SilverTourismTripDataObject.PARTITION_COLUMNS
    tourism_trips_do.write(partition_columns=partition_columns)

    ### Read MCC to ISO timezone mapping from provide file and write to test resources dir
    mcc_iso_timezones_path = config["Paths.Bronze"][mcc_iso_timezones_id]
    mcc_iso_timezones_do = BronzeMccIsoTzMap(spark, mcc_iso_timezones_path)
    mcc_iso_timezones_do.df = spark.read.parquet(mcc_iso_timezones_input_path)
    mcc_iso_timezones_do.write()


def data_test_0001() -> dict:
    date_format = "%Y-%m-%dT%H:%M:%S"

    input_time_segments_data = []
    input_trips_data = []
    output_trips_data = []
    output_aggs_data = []

    # Add user data
    input_time_segments_data, input_trips_data, output_trips_data, output_aggs_data = add_user_a(
        input_time_segments_data, input_trips_data, output_trips_data, output_aggs_data, date_format
    )

    return {
        input_time_segments_id: input_time_segments_data,
        input_tourism_trips_id: input_trips_data,
        expected_tourism_trips_id: output_trips_data,
        expected_tourism_outbound_nights_spent_id: output_aggs_data,
    }


def add_user_a(
    input_time_segments_data,
    input_trips_data,
    output_trips_data,
    output_aggs_data,
    date_format,
) -> tuple[list[Row], list[Row], list[Row], list[Row]]:
    """
    Add user A data.
    """
    user_id_a = "user_a".encode("ascii")
    user_id_modulo = 0
    mcc = 206
    mnc = "01"
    plmn_home = 20601
    plmn_abroad_a = 20811
    plmn_abroad_b = 212333
    dataset_id = ReservedDatasetIDs.ABROAD

    user_trips_data = [  # ongoing trips
        Row(
            user_id=user_id_a,
            trip_id="86d1c8910b61fa40373d758b9fde4395",
            trio_start_timestamp=datetime.strptime("2022-12-30T16:00:00", date_format),
            time_segment_ids_list=[2, 3],
            is_trip_finished=False,
            year=2022,
            month=12,
            user_id_modulo=user_id_modulo,
            dataset_id=dataset_id,
        ),
    ]

    user_time_segments_data = [
        Row(  # prev month, non-abroad
            user_id=user_id_a,
            time_segment_id=1,
            start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T15:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_home,
            cells=[],
            state=SegmentStates.STAY,
            is_last=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # prev month, abroad country a
            user_id=user_id_a,
            time_segment_id=2,
            start_timestamp=datetime.strptime("2022-12-30T16:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T17:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_a,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # prev month, abroad country b
            user_id=user_id_a,
            time_segment_id=3,
            start_timestamp=datetime.strptime("2022-12-30T18:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T19:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_b,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-01, abroad country a, continuing prev month trip
            user_id=user_id_a,
            time_segment_id=4,
            start_timestamp=datetime.strptime("2023-01-01T15:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-02T23:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_a,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-01, abroad country a (more than trip gap away from previous segment)
            user_id=user_id_a,
            time_segment_id=5,
            start_timestamp=datetime.strptime("2023-01-06T15:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-06T23:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_a,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=1,
            day=6,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-01, abroad country b
            user_id=user_id_a,
            time_segment_id=6,
            start_timestamp=datetime.strptime("2023-01-07T07:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-09T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_b,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=1,
            day=7,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-01, abroad country b, new trip near month end
            user_id=user_id_a,
            time_segment_id=7,
            start_timestamp=datetime.strptime("2023-01-29T07:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-31T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_b,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=1,
            day=29,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-02, abroad country b month start (continuing from prev month trip)
            user_id=user_id_a,
            time_segment_id=8,
            start_timestamp=datetime.strptime("2023-02-01T05:00:00", date_format),
            end_timestamp=datetime.strptime("2023-02-06T15:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_b,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=2,
            day=1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # month 2023-02, abroad country b (new trip)
            user_id=user_id_a,
            time_segment_id=9,
            start_timestamp=datetime.strptime("2023-02-11T05:00:00", date_format),
            end_timestamp=datetime.strptime("2023-02-11T23:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn_abroad_b,
            cells=[],
            state=SegmentStates.ABROAD,
            is_last=False,
            year=2023,
            month=2,
            day=11,
            user_id_modulo=user_id_modulo,
        ),
    ]

    user_output_trips_data = [
        Row(
            user_id=user_id_a,
            trip_id="86d1c8910b61fa40373d758b9fde4395",
            trip_start_timestamp=datetime.strptime("2022-12-30T16:00:00", date_format),
            time_segment_ids_list=[2, 3],
            is_trip_finished=False,
            year=2022,
            month=12,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
        Row(
            user_id=user_id_a,
            trip_id="86d1c8910b61fa40373d758b9fde4395",
            trip_start_timestamp=datetime.strptime("2022-12-30T16:00:00", date_format),
            time_segment_ids_list=[2, 3, 4],
            is_trip_finished=True,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
        Row(
            user_id=user_id_a,
            trip_id="d5e2930d48fbb003dc2e74bda778d802",
            trip_start_timestamp=datetime.strptime("2023-01-06T15:00:00", date_format),
            time_segment_ids_list=[5, 6],
            is_trip_finished=True,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
        Row(
            user_id=user_id_a,
            trip_id="b2bbb140888d39abac6405fa75ebcbd5",
            trip_start_timestamp=datetime.strptime("2023-01-29T07:00:00", date_format),
            time_segment_ids_list=[7, 8],
            is_trip_finished=False,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
        Row(
            user_id=user_id_a,
            trip_id="b2bbb140888d39abac6405fa75ebcbd5",
            trip_start_timestamp=datetime.strptime("2023-01-29T07:00:00", date_format),
            time_segment_ids_list=[7, 8],
            is_trip_finished=True,
            year=2023,
            month=2,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
        Row(
            user_id=user_id_a,
            trip_id="9c4b3562254a21c8a5c8241b82142275",
            trip_start_timestamp=datetime.strptime("2023-02-11T05:00:00", date_format),
            time_segment_ids_list=[9],
            is_trip_finished=True,
            year=2023,
            month=2,
            user_id_modulo=0,
            dataset_id=dataset_id,
        ),
    ]

    user_output_aggs_data = [
        Row(
            time_period="2023-01",
            country_of_destination="FR",
            nights_spent=1.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-01",
            country_of_destination="MC",
            nights_spent=1.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="MC",
            nights_spent=2.0,
            year=2023,
            month=2,
        ),
    ]

    input_time_segments_data += user_time_segments_data
    input_trips_data += user_trips_data
    output_trips_data += user_output_trips_data
    output_aggs_data += user_output_aggs_data

    return (
        input_time_segments_data,
        input_trips_data,
        output_trips_data,
        output_aggs_data,
    )
