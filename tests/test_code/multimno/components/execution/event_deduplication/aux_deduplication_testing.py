import pytest
from configparser import ConfigParser
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import pandas as pd

from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_deduplicated_events(spark):
    """
    Aux function to setup expected data.
    One row is kept in the case of same location duplicates..

    Args:
        spark (SparkSession): spark session

    Returns:
        DataFrame: expected data
    """

    expected_data = [
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 11, 9),
            mcc=154,
            cell_id="341098809306899",
            latitude=29.129932,
            longitude=12.52241,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 10, 24),
            mcc=154,
            cell_id="341098809306858",
            latitude=26.129932,
            longitude=12.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),
    ]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverEventDataObject.SCHEMA)

    return expected_data_df


def set_syntatically_cleaned_event_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data, that has been syntatically cleaned,
    but contains duplicates.
    In this case it will contain one duplicate of both cases, and one case of no duplicate.
    (one example of same location and one example of different location duplicate)

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["event_data_silver"]

    ### 5 rows: One correct, two with same location duplicate,
    # two with different location duplicate

    data = [
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 11, 9),
            mcc=154,
            cell_id="341098809306899",
            latitude=29.129932,
            longitude=12.52241,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),  # not a duplicate of any kind,
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 10, 24),
            mcc=154,
            cell_id="341098809306858",
            latitude=26.129932,
            longitude=12.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 10, 24),
            mcc=154,
            cell_id="341098809306858",
            latitude=26.129932,
            longitude=12.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),  # full duplicate of previous row, one row is kept
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 10, 19),
            mcc=154,
            cell_id="341098809306812",
            latitude=26.129932,
            longitude=12.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),
        Row(
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            timestamp=datetime(2023, 1, 1, 10, 19),
            mcc=154,
            cell_id="341098809306812",
            latitude=26.12925,
            longitude=52.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=399,
        ),  # different location duplicate with previous row
    ]

    input_data_df = spark.createDataFrame(data, schema=SilverEventDataObject.SCHEMA)
    ### Write input data in test resources dir
    input_data = SilverEventDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)


@pytest.fixture
def expected_qa_freq(spark: SparkSession):
    """
    Aux function to setup expected data for QA frequency
    In same location case: one row is kept, frequency difference is 1.
    In different location case: zero rows ar kept, frequency difference is 2.
    In no duplication case, initial and final frequency are equal.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    data = [
        Row(
            cell_id="341098809306899",
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            initial_frequency=1,
            final_frequency=1,
            date=date(2023, 1, 1),
        ),
        Row(
            cell_id="341098809306858",
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            initial_frequency=2,
            final_frequency=1,
            date=date(2023, 1, 1),
        ),
        Row(
            cell_id="341098809306812",
            user_id=bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            initial_frequency=2,
            final_frequency=0,
            date=date(2023, 1, 1),
        ),
    ]

    output_data_df = spark.createDataFrame(
        data, schema=SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA
    )

    return output_data_df


def generate_qa_col_values_for_dates_with_no_records(spark: SparkSession, config: ConfigParser):
    """Generates values for those dates that have been set in config
    for procesing, but actually hold no data in this test case (values will be 0
    for both error cases)

    Returns a dataframe, does not write to disk.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): parsed config

    Returns:
        _type_: DataFrame
    """

    data_period_start = config.get("EventDeduplication", "data_period_start")
    data_period_end = config.get("EventDeduplication", "data_period_end")
    data_folder_date_format = config.get("EventDeduplication", "data_folder_date_format")

    # Create all possible dates between start and end
    sdate = pd.to_datetime(data_period_start)
    edate = pd.to_datetime(data_period_end)
    to_process_dates = [x.date() for x in list(pd.date_range(sdate, edate, freq="d"))]
    to_process_dates = to_process_dates[1:]  # skip starting date, which was the current date for actual duplicates

    data = []

    for selected_date in to_process_dates:
        data.append(
            Row(
                result_timestamp=datetime.now(),
                date=selected_date,
                variable=None,
                type_of_error=11,
                type_of_transformation=None,
                value=0,
            )
        )

        data.append(
            Row(
                result_timestamp=datetime.now(),
                date=selected_date,
                variable=None,
                type_of_error=10,
                type_of_transformation=None,
                value=0,
            )
        )

    output_data_df = spark.createDataFrame(data, schema=SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA)

    return output_data_df


@pytest.fixture
def expected_qa_by_col(spark: SparkSession):
    """
    Aux function to setup expected data for QA by column

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    data = [
        # Metrics for one day, with both types representad and both value cases represented
        # Day where duplicates exist
        Row(
            result_timestamp=datetime.now(),
            date=date(2023, 1, 1),
            variable=None,
            type_of_error=11,
            type_of_transformation=None,
            value=1,
        ),
        Row(
            result_timestamp=datetime.now(),
            date=date(2023, 1, 1),
            variable=None,
            type_of_error=10,
            type_of_transformation=None,
            value=2,
        ),
    ]

    output_data_df = spark.createDataFrame(data, schema=SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA)

    return output_data_df
