import pytest
from configparser import ConfigParser
import datetime
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
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
    BinaryType,
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
from tests.test_code.multimno.components.execution.daily_permanence_score.aux_dps_testing import (
    DPS_AUX_SCHEMA,
    get_cellfootprint_testing_df,
)
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
            ColNames.cell_id: "106927944066972",
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 8, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.stay_duration: 30.0 * 60,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: "cell",
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.cell_id: "106927944066972",
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 10, 0, 0),
            ColNames.stay_duration: 15.0 * 60,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: "cell",
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.cell_id: "106927944066972",
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 2, 9, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 2, 10, 0, 0),
            ColNames.stay_duration: 20.0 * 60,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 2,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: "cell",
        },
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.cell_id: "106927944066972",
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 3, 17, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 3, 18, 0, 0),
            ColNames.stay_duration: 45.0 * 60,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 3,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: "cell",
        },  # next day
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.cell_id: "106927944066972",
            ColNames.time_slot_initial_time: datetime.datetime(2023, 1, 3, 18, 0, 0),
            ColNames.time_slot_end_time: datetime.datetime(2023, 1, 3, 19, 0, 0),
            ColNames.stay_duration: 10.0 * 60,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day: 3,
            ColNames.user_id_modulo: 1,
            ColNames.id_type: "cell",
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


# --- Expected ---


def get_expected_midterm_df(spark):
    expected_midterm_ps_data = [
        {
            ColNames.user_id: get_user_id_hashed("1"),
            ColNames.grid_id: "100mN100E100",
            ColNames.mps: 2,
            ColNames.frequency: 2,
            ColNames.regularity_mean: 14.666667,
            ColNames.regularity_std: 17.953644,
            ColNames.year: 2023,
            ColNames.month: 1,
            ColNames.day_type: "all",
            ColNames.time_interval: "all",
            ColNames.id_type: "grid",
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
    cell_footprint_path = config["Paths.Silver"]["cell_footprint_data_silver"]

    # ---------------- Daily permanence score data object -----------------
    dps_df = get_dps_testing_df(spark)

    # ---------------- Holiday calendar data object -----------------
    holiday_df = get_holiday_testing_df(spark)
    # ---------------- Cell Footprint -----------------
    cell_footprint_df = get_cellfootprint_testing_df(spark)

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

    # Cell Footprint
    cell_footprint_do = SilverCellFootprintDataObject(spark, cell_footprint_path)
    cell_footprint_do.df = cell_footprint_df
    cell_footprint_do.write()


# --------- Expected Fixture ---------
@pytest.fixture
def expected_midterm_ps(spark):
    return get_expected_midterm_df(spark)
