import pytest
from configparser import ConfigParser
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    Row,
    StringType,
    StructField,
    StructType,
    IntegerType,
    TimestampType,
    ShortType,
    ByteType,
    DateType,
)
import pyspark.sql.functions as F

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_holiday_calendar_data_object import (
    BronzeHolidayCalendarDataObject,
)
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)

from multimno.core.data_objects.silver.silver_midterm_permanence_score_data_object import (
    SilverMidtermPermanenceScoreDataObject,
)

from tests.test_code.fixtures import spark_session as spark

fixtures = [spark]


@pytest.fixture
def expected_midterm_ps(spark):

    expected_data_list = [
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=1,
            frequency=1,
            regularity_mean=22.0,
            regularity_std=31.11269760131836,
            year=2023,
            month=1,
            day_type="all",
            time_interval="evening_time",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=1,
            frequency=1,
            regularity_mean=22.0,
            regularity_std=31.11269760131836,
            year=2023,
            month=1,
            day_type="all",
            time_interval="night_time",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=1,
            frequency=1,
            regularity_mean=22.0,
            regularity_std=31.11269760131836,
            year=2023,
            month=1,
            day_type="workdays",
            time_interval="night_time",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=2,
            frequency=1,
            regularity_mean=22.0,
            regularity_std=31.11269760131836,
            year=2023,
            month=1,
            day_type="all",
            time_interval="working_hours",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=2,
            frequency=1,
            regularity_mean=22.0,
            regularity_std=31.11269760131836,
            year=2023,
            month=1,
            day_type="weekends",
            time_interval="all",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="100mN4056000E5275300",
            mps=3,
            frequency=2,
            regularity_mean=14.666666984558105,
            regularity_std=17.785762786865234,
            year=2023,
            month=1,
            day_type="all",
            time_interval="all",
            id_type="grid",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=1,
            frequency=1,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="all",
            time_interval="evening_time",
            id_type="device_observation",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=1,
            frequency=1,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="all",
            time_interval="night_time",
            id_type="device_observation",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=1,
            frequency=1,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="workdays",
            time_interval="night_time",
            id_type="device_observation",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=2,
            frequency=1,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="all",
            time_interval="working_hours",
            id_type="device_observation",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=2,
            frequency=1,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="weekends",
            time_interval="all",
            id_type="device_observation",
            user_id_modulo=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            grid_id="device_observation",
            mps=3,
            frequency=2,
            regularity_mean=None,
            regularity_std=None,
            year=2023,
            month=1,
            day_type="all",
            time_interval="all",
            id_type="device_observation",
            user_id_modulo=1,
        ),
    ]
    expected_data_df = spark.createDataFrame(expected_data_list, schema=SilverMidtermPermanenceScoreDataObject.SCHEMA)

    return expected_data_df


def set_input_data(spark: SparkSession, config: ConfigParser):
    """"""
    dps_test_data_path = config["Paths.Silver"]["daily_permanence_score_data_silver"]
    holiday_data_path = config["Paths.Bronze"]["holiday_calendar_data_bronze"]

    dps_data_list = [
        [
            1,
            "100mN4056000E5275300",
            datetime.datetime(2023, 1, 1, 8, 0, 0),
            datetime.datetime(2023, 1, 1, 9, 0, 0),
            0,
            2023,
            1,
            1,
            1,
            "grid",
        ],
        [
            1,
            "100mN4056000E5275300",
            datetime.datetime(2023, 1, 1, 9, 0, 0),
            datetime.datetime(2023, 1, 1, 10, 0, 0),
            1,
            2023,
            1,
            1,
            1,
            "grid",
        ],
        [
            1,
            "100mN4056000E5275300",
            datetime.datetime(2023, 1, 1, 10, 0, 0),
            datetime.datetime(2023, 1, 1, 11, 0, 0),
            1,
            2023,
            1,
            1,
            1,
            "grid",
        ],
        [
            1,
            "100mN4056000E5275300",
            datetime.datetime(2023, 1, 3, 17, 0, 0),
            datetime.datetime(2023, 1, 3, 18, 0, 0),
            0,
            2023,
            1,
            3,
            1,
            "grid",
        ],  # next day
        [
            1,
            "100mN4056000E5275300",
            datetime.datetime(2023, 1, 3, 18, 0, 0),
            datetime.datetime(2023, 1, 3, 19, 0, 0),
            1,
            2023,
            1,
            3,
            1,
            "grid",
        ],  # next day
    ]

    daily_ps_df = [
        Row(
            **{
                ColNames.user_id: dpsdata[0],
                ColNames.grid_id: dpsdata[1],
                ColNames.time_slot_initial_time: dpsdata[2],
                ColNames.time_slot_end_time: dpsdata[3],
                ColNames.dps: dpsdata[4],
                ColNames.year: dpsdata[5],
                ColNames.month: dpsdata[6],
                ColNames.day: dpsdata[7],
                ColNames.user_id_modulo: dpsdata[8],
                ColNames.id_type: dpsdata[9],
            }
        )
        for dpsdata in dps_data_list
    ]

    TEMP_SCHEMA = StructType(
        [
            StructField(ColNames.user_id, IntegerType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.time_slot_initial_time, TimestampType(), nullable=False),
            StructField(ColNames.time_slot_end_time, TimestampType(), nullable=False),
            StructField(ColNames.dps, ByteType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
        ]
    )

    dps_df = spark.createDataFrame(daily_ps_df, schema=TEMP_SCHEMA)

    dps_df = (
        dps_df.withColumn("hashed", F.sha2(F.col(ColNames.user_id).cast(StringType()), 256))
        .withColumn(ColNames.user_id, F.unhex(F.col("hashed")))
        .drop("hashed")
    )

    # ---------------- Holiday calendar data object -----------------

    holiday_data_list = ["IT", datetime.date(2023, 12, 24), "Christmas"]

    HOLIDAY_SCHEMA = StructType(
        [
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=False),
        ]
    )

    holiday_df = spark.createDataFrame([holiday_data_list], schema=HOLIDAY_SCHEMA)

    # ---------------- Writing -----------------

    # Write input data in test resources dir

    dps_data = SilverDailyPermanenceScoreDataObject(spark, dps_test_data_path)
    dps_data.df = dps_df
    dps_data.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])

    holiday_data = BronzeHolidayCalendarDataObject(spark, holiday_data_path)
    holiday_data.df = holiday_df
    holiday_data.write()
