import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from hashlib import sha256
import pyspark.sql.functions as F
from datetime import datetime, date

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
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


@pytest.fixture(scope="module")
def expected_events(spark):
    date_format = "%Y-%m-%d %H:%M:%S"

    expected_data = [
        [
            sha256(b"1").digest(),
            datetime.strptime("2023-01-01 13:00:00", date_format),
            123,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
            0,
        ],  # OK
        [
            sha256(b"1").digest(),
            datetime.strptime("2023-01-01 13:08:00", date_format),
            123,
            None,
            0.0,
            0.0,
            100.0,
            2023,
            1,
            1,
            0,
        ],
    ]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverEventDataObject.SCHEMA)
    return expected_data_df


@pytest.fixture(scope="module")
def expected_frequency_distribution(spark):
    expected_data = [
        [None, sha256(b"1").digest(), 3, 1, date(2023, 1, 1)],
        [1, sha256(b"1").digest(), 1, 0, date(2023, 1, 1)],
        ["100000000000000", None, 1, 0, date(2023, 1, 1)],
        ["100000000000000", sha256(b"1").digest(), 2, 1, date(2023, 1, 1)],
    ]

    expected_data_df = spark.createDataFrame(
        expected_data, schema=SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA
    )
    return expected_data_df


@pytest.fixture(scope="module")
def expected_quality_metrics_by_column(spark):
    now = datetime.now()
    correct_date = date(2023, 1, 1)

    expected_data = [
        [now, correct_date, ColNames.timestamp, None, 1, 3],
        [now, correct_date, ColNames.timestamp, 2, None, 0],
        [now, correct_date, ColNames.timestamp, 3, None, 0],
        [now, correct_date, ColNames.timestamp, 9, None, 3],
        [now, correct_date, None, 6, None, 1],
        [now, correct_date, ColNames.user_id, 1, None, 1],
        [now, correct_date, ColNames.user_id, 9, None, 6],
        [now, correct_date, ColNames.timestamp, 1, None, 0],
        [now, correct_date, None, 5, None, 1],
        [now, correct_date, ColNames.cell_id, 3, None, 1],
        [now, correct_date, ColNames.cell_id, 9, None, 3],
        [now, correct_date, ColNames.mcc, 1, None, 0],
        [now, correct_date, ColNames.mcc, 3, None, 1],
        [now, correct_date, ColNames.mcc, 9, None, 5],
    ]

    expected_data_df = spark.createDataFrame(
        expected_data, schema=SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA
    )
    expected_data_df = expected_data_df.drop(ColNames.result_timestamp)
    return expected_data_df


def write_input_event_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    test_data_path = config["Paths.Bronze"]["event_data_bronze"]
    d = [
        [sha256(b"1").digest(), "2023-01-01 15:00:00", 123, "100000000000000", None, None, None, 2023, 1, 1],  # OK
        [None, "2023-01-01 15:01:00", 123, "100000000000000", None, None, None, 2023, 1, 1],  # Filtered as null
        [
            sha256(b"1").digest(),
            "2023-01-01 15:02:00",
            3,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Invalid mcc
        [sha256(b"1").digest(), "2023-01-01 15:03:00", 123, "1", None, None, None, 2023, 1, 1],  # Invalid cell_id
        [sha256(b"1").digest(), "2023-000", 123, "100000000000000", None, None, None, 2023, 1, 1],  # Invalid timestamp
        [
            sha256(b"1").digest(),
            "2023-01-02 15:05:00",
            123,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Data period filter
        [sha256(b"1").digest(), "2023-01-01 15:06:00", 123, None, 1000.0, 0.0, None, 2023, 1, 1],  # Bounding box filter
        [sha256(b"1").digest(), "2023-01-01 15:07:00", 123, None, None, None, None, 2023, 1, 1],  # No location
        [sha256(b"1").digest(), "2023-01-01 15:08:00", 123, None, 0.0, 0.0, 100.0, 2023, 1, 1],
    ]
    input_data_df = spark.createDataFrame(data=d, schema=BronzeEventDataObject.SCHEMA)
    input_data_df = input_data_df.withColumns(
        {
            ColNames.year: F.year(ColNames.timestamp).cast("smallint"),
            ColNames.month: F.month(ColNames.timestamp).cast("tinyint"),
            ColNames.day: F.dayofmonth(ColNames.timestamp).cast("tinyint"),
        }
    )
    ### Write input data in test resources dir
    input_data = BronzeEventDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])
