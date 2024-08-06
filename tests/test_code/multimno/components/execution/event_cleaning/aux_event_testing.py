import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from hashlib import sha256
import pyspark.sql.functions as F
from datetime import datetime, date

from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import ErrorTypes
from multimno.core.constants.transformations import Transformations
from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)
from multimno.core.data_objects.silver.silver_event_data_object import (
    SilverEventDataObject,
)
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
            bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            datetime.strptime("2023-01-01 13:13:00", date_format),
            # datetime(2023, 1, 1, 15, 23),
            154,
            "01",
            None,
            "341098809306858",
            26.129932,
            12.52221,
            None,
            2023,
            1,
            1,
            1,
        ],  # deduplication result, one row is kept
        [
            sha256(b"1").digest(),
            datetime.strptime("2023-01-01 13:00:00", date_format),
            123,
            "01",
            None,
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
            "01",
            None,
            None,
            0.0,
            0.0,
            100.0,
            2023,
            1,
            1,
            0,
        ],
        [
            sha256(b"1").digest(),
            datetime.strptime("2023-01-01 14:00:00", date_format),
            None,
            None,
            12301,
            None,
            None,
            None,
            None,
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
        [
            "341098809306858",
            bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            2,
            1,
            date(2023, 1, 1),
        ],
        [None, sha256(b"1").digest(), 7, 2, date(2023, 1, 1)],
        [1, sha256(b"1").digest(), 1, 0, date(2023, 1, 1)],
        ["100000000000000", None, 1, 0, date(2023, 1, 1)],
        ["100000000000000", sha256(b"1").digest(), 2, 1, date(2023, 1, 1)],
    ]

    expected_data_df = spark.createDataFrame(
        expected_data,
        schema=SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA,
    )
    return expected_data_df


@pytest.fixture(scope="module")
def expected_quality_metrics_by_column(spark):
    now = datetime.now()
    correct_date = date(2023, 1, 1)

    expected_data = [
        [now, correct_date, ColNames.timestamp, None, Transformations.converted_timestamp, 6],
        [now, correct_date, ColNames.timestamp, ErrorTypes.missing_value, None, 0],
        [now, correct_date, ColNames.timestamp, ErrorTypes.not_right_syntactic_format, None, 0],
        [now, correct_date, ColNames.timestamp, ErrorTypes.out_of_admissible_values, None, 0],
        [now, correct_date, ColNames.timestamp, ErrorTypes.no_error, None, 6],
        [now, correct_date, ColNames.user_id, ErrorTypes.missing_value, None, 1],
        [now, correct_date, ColNames.user_id, ErrorTypes.no_error, None, 12],
        [now, correct_date, ColNames.cell_id, ErrorTypes.out_of_admissible_values, None, 1],
        [now, correct_date, ColNames.cell_id, ErrorTypes.no_error, None, 6],
        [now, correct_date, ColNames.mcc, 3, None, 1],
        [now, correct_date, ColNames.mcc, 9, None, 8],
        [now, correct_date, ColNames.mnc, 3, None, 1],
        [now, correct_date, ColNames.mnc, 9, None, 7],
        [now, correct_date, ColNames.plmn, 3, None, 1],
        [now, correct_date, ColNames.plmn, 9, None, 1],
        [now, correct_date, None, ErrorTypes.no_location, None, 1],
        [now, correct_date, None, ErrorTypes.out_of_bounding_box, None, 1],
        [now, correct_date, None, ErrorTypes.no_domain, None, 1],
        [now, correct_date, None, ErrorTypes.same_location_duplicate, None, 1],
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
        [
            sha256(b"1").digest(),
            "2023-01-01 15:00:00",
            123,
            "01",
            None,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # OK
        [
            None,
            "2023-01-01 15:01:00",
            123,
            "01",
            None,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Filtered as null
        [
            sha256(b"1").digest(),
            "2023-01-01 15:02:00",
            3,
            "01",
            None,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Invalid mcc
        [
            sha256(b"1").digest(),
            "2023-01-01 15:03:00",
            123,
            "01",
            None,
            "1",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Invalid cell_id
        [
            sha256(b"1").digest(),
            "2023-000",
            123,
            "01",
            None,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Invalid timestamp
        [
            sha256(b"1").digest(),
            "2023-01-02 15:05:00",
            123,
            "01",
            None,
            "100000000000000",
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Data period filter
        [
            sha256(b"1").digest(),
            "2023-01-01 15:06:00",
            123,
            "01",
            None,
            None,
            1000.0,
            0.0,
            None,
            2023,
            1,
            1,
        ],  # Bounding box filter
        [
            sha256(b"1").digest(),
            "2023-01-01 15:07:00",
            123,
            "01",
            None,
            None,
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # No location
        [
            sha256(b"1").digest(),
            "2023-01-01 15:08:00",
            123,
            "01",
            None,
            None,
            0.0,
            0.0,
            100.0,
            2023,
            1,
            1,
        ],
        # Duplicate of next row
        [
            bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            "2023-01-01 15:13:00",  # datetime.strptime("2023-01-01 15:23:00", date_format), #datetime(2023, 1, 1, 15, 23),
            154,
            "01",
            None,
            "341098809306858",
            26.129932,
            12.52221,
            None,
            2023,
            1,
            1,
        ],
        [
            bytearray(
                b'\x00\xa1\x07\xa3I\x8f\xa2\xa8\x17h\x1c\xaf\xeejQ\xa1}\xe3w\x05\xbf\x97m\xbeL\x9f\x03\xf1"W|\xd3'
            ),
            "2023-01-01 15:13:00",  # , date_format), datetime(2023, 1, 1, 15, 23),
            154,
            "01",
            None,
            "341098809306858",
            26.129932,
            12.52221,
            None,
            2023,
            1,
            1,
        ],
        [
            sha256(b"1").digest(),
            "2023-01-01 15:08:00",
            None,
            None,
            None,
            None,
            0.0,
            0.0,
            100.0,
            2023,
            1,
            1,
        ],  # No domain
        [
            sha256(b"1").digest(),
            "2023-01-01 15:08:00",
            123,
            "aa",
            None,
            None,
            0.0,
            0.0,
            100.0,
            2023,
            1,
            1,
        ],  # Invalid mnc
        [
            sha256(b"1").digest(),
            "2023-01-01 15:00:00",
            None,
            None,
            1230123,
            None,
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Invalid plmn
        [
            sha256(b"1").digest(),
            "2023-01-01 16:00:00",
            None,
            None,
            12301,
            None,
            None,
            None,
            None,
            2023,
            1,
            1,
        ],  # Valid outbound
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
