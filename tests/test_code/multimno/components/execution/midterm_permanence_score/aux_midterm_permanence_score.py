import pytest
import datetime as dt
from configparser import ConfigParser
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    Row,
    StringType,
    StructField,
    StructType,
    IntegerType,
    ArrayType,
    DateType,
    BinaryType,
    DoubleType,
)
import pyspark.sql.functions as F

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_holiday_calendar_data_object import (
    BronzeHolidayCalendarDataObject,
)
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_group_to_tile_data_object import SilverGroupToTileDataObject

from multimno.core.data_objects.silver.silver_midterm_permanence_score_data_object import (
    SilverMidtermPermanenceScoreDataObject,
)
from multimno.core.constants.error_types import UeGridIdType

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_utils import get_user_id_hashed

fixtures = [spark]

# --------- Data Objects ---------


# --- Input Data ---
def get_dps_testing_df(spark: SparkSession):
    # stay duration in seconds
    # 2023, 1, 2 = monday
    dps_data = [
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 8, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.dps: [1],
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: UeGridIdType.GRID_STR,
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 10, 0, 0),
            ColNames.dps: [1],
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: UeGridIdType.GRID_STR,
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 10, 0, 0),
            ColNames.dps: [1],
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: UeGridIdType.GRID_STR,
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 3, 17, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 3, 18, 0, 0),
            ColNames.dps: [1],
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 3,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: UeGridIdType.GRID_STR,
        },  # next day
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 3, 18, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 3, 19, 0, 0),
            ColNames.dps: [1],
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 3,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: UeGridIdType.GRID_STR,
        },  # next day
    ]

    # Create df
    dps_df = spark.createDataFrame([Row(**row) for row in dps_data], schema=SilverDailyPermanenceScoreDataObject.SCHEMA)

    dps_do = SilverDailyPermanenceScoreDataObject(spark, None)
    dps_do.df = dps_df
    dps_do.cast_to_schema()

    return dps_do.df


def get_holiday_testing_df(spark):
    holiday_data_list = ["IT", datetime.date(2023, 12, 24), "Christmas"]

    HOLIDAY_SCHEMA = StructType(
        [
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=False),
        ]
    )

    holiday_df = spark.createDataFrame([holiday_data_list], schema=HOLIDAY_SCHEMA)

    return holiday_df


def get_input_group_to_tile_df(spark: SparkSession):
    data = [{ColNames.group_id: 1, ColNames.grid_id: 0}]

    return spark.createDataFrame(data, SilverGroupToTileDataObject.SCHEMA)


# --- Expected ---


def get_expected_midterm_df(spark):
    expected_midterm_ps_data = [
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.grid_id: 0,
            ColNames.mps: 5,
            ColNames.frequency: 2,
            ColNames.regularity_mean: None,
            ColNames.regularity_std: None,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day_type: "all",
            ColNames.time_interval: "all",
            ColNames.id_type: UeGridIdType.GRID_STR,
            ColNames.user_id_modulo: 1,
        }
    ]
    # TODO: more accurate comparison
    return spark.createDataFrame(
        [Row(**row) for row in expected_midterm_ps_data], schema=SilverMidtermPermanenceScoreDataObject.SCHEMA
    )


# --- Input Data Writing ---


def set_input_data(spark: SparkSession, config: ConfigParser):
    """"""
    dps_test_data_path = config["Paths.Silver"]["daily_permanence_score_data_silver"]
    holiday_data_path = config["Paths.Bronze"]["holiday_calendar_data_bronze"]
    group_to_tile_path = config["Paths.Silver"]["group_to_tile_data_silver"]

    # ---------------- Daily permanence score data object -----------------
    dps_df = get_dps_testing_df(spark)

    # ---------------- Holiday calendar data object -----------------
    holiday_df = get_holiday_testing_df(spark)

    group_to_tile = get_input_group_to_tile_df(spark)

    # ---------------- Writing -----------------

    # Write input data in test resources dir
    # DPS
    dps_do = SilverDailyPermanenceScoreDataObject(spark, dps_test_data_path)
    dps_do.df = dps_df
    dps_do.write()

    # Holiday
    holiday_do = BronzeHolidayCalendarDataObject(spark, holiday_data_path)
    holiday_do.df = holiday_df
    holiday_do.write()

    group_to_tile_do = SilverGroupToTileDataObject(spark, group_to_tile_path)
    group_to_tile_do.df = group_to_tile
    group_to_tile_do.write()


# --------- Expected Fixture ---------
@pytest.fixture
def expected_midterm_ps(spark):
    return get_expected_midterm_df(spark)


# --------- Metrics calculation Testing Data ---------
def get_metrics_calculation_input_and_expected(spark: SparkSession):
    # Col names
    dates_col = "dates"

    # ------- Input -------
    # Data
    user_id = "1"
    user_id_modulo = 1
    group_ids = [
        1,
        "1111111111111111111111111111111111111111111111111111111111111111",
    ]
    mps_values = [1, 2]
    dates_vales = [
        [dt.date(2021, 2, 2), dt.date(2021, 2, 3)],  # Two days in study month
        [
            dt.date(2021, 1, 30),
            dt.date(2021, 2, 2),
            dt.date(2021, 3, 1),
        ],  # One day in study month, one day in before buffer and one day in after buffer
    ]

    data = [
        {
            ColNames.user_id_modulo: user_id_modulo,
            ColNames.user_id: get_user_id_hashed(user_id),
            ColNames.group_id: group_id,
            ColNames.mps: mps,
            dates_col: dates,
        }
        for group_id, mps, dates in zip(group_ids, mps_values, dates_vales)
    ]

    schema = StructType(
        [
            StructField(ColNames.user_id_modulo, IntegerType(), True),
            StructField(ColNames.user_id, BinaryType(), True),
            StructField(ColNames.group_id, StringType(), True),
            StructField(ColNames.mps, IntegerType(), True),
            StructField(dates_col, ArrayType(DateType()), True),
        ]
    )

    input_df = spark.createDataFrame(data, schema=schema)

    # -------- Expected --------
    frequency = [2, 3]
    regularity_mean = [19.67, 15.0]
    regularity_std = [19.55, 16.97]
    id_type = UeGridIdType.GRID_STR

    data = [
        {
            ColNames.user_id_modulo: user_id_modulo,
            ColNames.user_id: get_user_id_hashed(user_id),
            ColNames.group_id: group_id,
            ColNames.mps: mps,
            ColNames.frequency: frequency,
            ColNames.regularity_mean: regularity_mean,
            ColNames.regularity_std: regularity_std,
            ColNames.id_type: id_type,
        }
        for group_id, mps, frequency, regularity_mean, regularity_std in zip(
            group_ids, mps_values, frequency, regularity_mean, regularity_std
        )
    ]

    expected_schema = StructType(
        [
            StructField(ColNames.user_id_modulo, IntegerType(), True),
            StructField(ColNames.user_id, BinaryType(), True),
            StructField(ColNames.group_id, StringType(), True),
            StructField(ColNames.mps, IntegerType(), True),
            StructField(ColNames.frequency, IntegerType(), False),
            StructField(ColNames.regularity_mean, DoubleType(), True),
            StructField(ColNames.regularity_std, DoubleType(), True),
            StructField(ColNames.id_type, StringType(), False),
        ]
    )

    expected_df = spark.createDataFrame(data, schema=expected_schema)

    return input_df, expected_df
