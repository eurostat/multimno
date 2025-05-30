import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    ShortType,
    ByteType,
    BooleanType,
    ArrayType,
)

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_cell_to_group_data_object import SilverCellToGroupDataObject

# from multimno.core.data_objects.silver.silver_group_to_tile_data_object import SilverGroupToTileDataObject
from multimno.core.data_objects.silver.silver_event_flagged_data_object import SilverEventFlaggedDataObject
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.event_cache_data_object import EventCacheDataObject

from tests.test_code.multimno.components.execution.daily_permanence_score.reference_data import (
    EVENTS,
    CACHE_EVENTS,
    CELL_FOOTPRINT,
    CELL_TO_GROUP,
    DPS,
)

from tests.test_code.fixtures import spark_session as spark
from datetime import datetime, timedelta

from multimno.core.data_objects.silver.event_cache_data_object import EventCacheDataObject
from tests.test_code.test_utils import get_user_id_hashed

# Dummy to avoid linting errors using pytest
fixtures = [spark]

# TODO: Setup user_id in reference data to avoid using aux schemas
EVENTS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.timestamp, StringType(), nullable=False),
        StructField(ColNames.mcc, IntegerType(), nullable=False),
        StructField(ColNames.plmn, IntegerType(), nullable=True),
        StructField(ColNames.cell_id, StringType(), nullable=True),
        StructField(ColNames.latitude, FloatType(), nullable=True),
        StructField(ColNames.longitude, FloatType(), nullable=True),
        StructField(ColNames.loc_error, FloatType(), nullable=True),
        StructField(ColNames.error_flag, IntegerType(), nullable=False),
        StructField(ColNames.year, ShortType(), nullable=False),
        StructField(ColNames.month, ByteType(), nullable=False),
        StructField(ColNames.day, ByteType(), nullable=False),
        StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
    ]
)

CACHE_EVENTS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.timestamp, StringType(), nullable=False),
        StructField(ColNames.mcc, IntegerType(), nullable=False),
        StructField(ColNames.plmn, IntegerType(), nullable=True),
        StructField(ColNames.cell_id, StringType(), nullable=True),
        StructField(ColNames.latitude, FloatType(), nullable=True),
        StructField(ColNames.longitude, FloatType(), nullable=True),
        StructField(ColNames.loc_error, FloatType(), nullable=True),
        StructField(ColNames.error_flag, IntegerType(), nullable=False),
        StructField(ColNames.year, ShortType(), nullable=False),
        StructField(ColNames.month, ByteType(), nullable=False),
        StructField(ColNames.day, ByteType(), nullable=False),
        StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        StructField(ColNames.is_last_event, BooleanType(), nullable=False),
    ]
)

DPS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.dps, ArrayType(StringType()), nullable=False),
        StructField(ColNames.time_slot_initial_time, StringType(), nullable=False),
        StructField(ColNames.time_slot_end_time, StringType(), nullable=False),
        StructField(ColNames.year, ShortType(), nullable=False),
        StructField(ColNames.month, ByteType(), nullable=False),
        StructField(ColNames.day, ByteType(), nullable=False),
        StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        StructField(ColNames.id_type, StringType(), nullable=True),
    ]
)


def cast_user_id_to_binary(df: DataFrame):
    """
    Convert PySpark DataFrame 'user_id' column to binary.

    Args:
        df (DataFrame): input dataframe.

    Returns:
        (DataFrame): output dataframe.
    """
    df = (
        df.withColumn("hashed_user_id", F.sha2(df[ColNames.user_id].cast("string"), 256))
        .withColumn(ColNames.user_id, F.unhex(F.col("hashed_user_id")))
        .drop("hashed_user_id")
    )
    return df


def cast_timestamp_field(df: DataFrame, colname: str):
    """
    Convert PySpark DataFrame string column to timestamp.
    The string column shall have "yyyy-MM-ddTHH:mm:ss" format.

    Args:
        df (DataFrame): input dataframe.
        colname (str): name of timestamp column.

    Returns:
        (DataFrame): output dataframe.
    """
    df = df.withColumn(colname, F.to_timestamp(colname, "yyyy-MM-dd'T'HH:mm:ss"))
    return df


@pytest.fixture
def expected_data(spark: SparkSession):
    """
    Aux function to setup expected data using reference data file.

    Args:
        spark (SparkSession): spark session.
    """
    return get_expected_dps_df(spark)


def get_expected_dps_df(spark: SparkSession):
    # Quick fix to add id_type to dummy DPS data
    # [x.update({ColNames.id_type: "cell"}) for x in DPS]
    expected_df = spark.createDataFrame([Row(**el) for el in DPS], DPS_AUX_SCHEMA)
    expected_df = cast_timestamp_field(cast_user_id_to_binary(expected_df), ColNames.time_slot_initial_time)
    expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_end_time)
    expected_df = expected_df.select(SilverDailyPermanenceScoreDataObject.SCHEMA.fieldNames())
    expected_df = spark.createDataFrame(expected_df.rdd, SilverDailyPermanenceScoreDataObject.SCHEMA)
    expected_df = expected_df.withColumn(ColNames.dps, F.array_sort(ColNames.dps))
    return expected_df


def get_cellfootprint_testing_df(spark: SparkSession):
    return spark.createDataFrame([Row(**el) for el in CELL_FOOTPRINT], SilverCellFootprintDataObject.SCHEMA)


def generate_input_cell_to_group_df(spark: SparkSession):
    return spark.createDataFrame([Row(**el) for el in CELL_TO_GROUP], SilverCellToGroupDataObject.SCHEMA)


# def generate_input_group_to_tile_df(spark: SparkSession):
#     return spark.createDataFrame([Row(**el) for el in GROUP_TO_TILE], SilverGroupToTileDataObject.SCHEMA)


def set_input_data(spark: SparkSession, config: ConfigParser):
    """
    Load input data (cell footprint and flagged events) and write to expected path.

    Args:
        spark (SparkSession): spark session.
        config (ConfigParser): component config.
    """
    # Cell footprint data
    test_data_path = config["Paths.Silver"]["cell_footprint_data_silver"]
    input_cells_df = get_cellfootprint_testing_df(spark)
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_cells_df
    input_data.write()

    # Cell to group data
    test_data_path = config["Paths.Silver"]["cell_to_group_data_silver"]
    input_cell_to_group_df = generate_input_cell_to_group_df(spark)
    input_data = SilverCellToGroupDataObject(spark, test_data_path)
    input_data.df = input_cell_to_group_df
    input_data.write()

    # # Group to tile data
    # test_data_path = config["Paths.Silver"]["group_to_tile_data_silver"]
    # input_group_to_tile_df = generate_input_group_to_tile_df(spark)
    # input_data = SilverGroupToTileDataObject(spark, test_data_path)
    # input_data.df = input_group_to_tile_df
    # input_data.write()

    # Event data
    test_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    input_events_df = cast_timestamp_field(
        cast_user_id_to_binary(spark.createDataFrame([Row(**el) for el in EVENTS], EVENTS_AUX_SCHEMA)),
        ColNames.timestamp,
    )
    input_data = SilverEventFlaggedDataObject(spark, test_data_path)
    input_data.df = input_events_df
    input_data.write()

    # Event cache data
    test_data_path = config["Paths.Silver"]["event_cache"]
    input_events_df = cast_timestamp_field(
        cast_user_id_to_binary(spark.createDataFrame([Row(**el) for el in CACHE_EVENTS], CACHE_EVENTS_AUX_SCHEMA)),
        ColNames.timestamp,
    )
    input_data = EventCacheDataObject(spark, test_data_path)
    input_data.df = input_events_df
    input_data.write()


# TEST DATA: EVENT LOADING
def get_event_load_testing_data(spark: SparkSession):
    # Test load data (only relevant columns)
    users = ["1", "2", "3"]
    n_events = 3
    stimestamp = "2023-01-02T00:05:00"
    timestamp = datetime.strptime(stimestamp, "%Y-%m-%dT%H:%M:%S")
    cell_id = "106927944066972"
    error_flag = 0

    EVENT_DATA = [
        {
            ColNames.user_id: get_user_id_hashed(user),
            ColNames.timestamp: timestamp + timedelta(minutes=i),
            ColNames.cell_id: cell_id,
            ColNames.error_flag: error_flag,
            ColNames.year: timestamp.year,
            ColNames.month: timestamp.month,
            ColNames.day: timestamp.day,
            ColNames.user_id_modulo: int(user),
        }
        for user in users
        for i in range(n_events)
    ]

    CACHE_EVENT_DATA = []
    for buffer_timestamp, is_last_event in zip(
        [timestamp - timedelta(days=1), timestamp + timedelta(days=1)], [True, False]
    ):
        CACHE_EVENT_DATA.extend(
            [
                {
                    ColNames.user_id: get_user_id_hashed(user),
                    ColNames.timestamp: buffer_timestamp,
                    ColNames.cell_id: cell_id,
                    ColNames.error_flag: error_flag,
                    ColNames.year: buffer_timestamp.year,
                    ColNames.month: buffer_timestamp.month,
                    ColNames.day: buffer_timestamp.day,
                    ColNames.user_id_modulo: int(user),
                    ColNames.is_last_event: is_last_event,
                }
                for user in users
            ]
        )

    events_df = spark.createDataFrame(EVENT_DATA, schema=SilverEventFlaggedDataObject.SCHEMA)

    cache_events_df = spark.createDataFrame(CACHE_EVENT_DATA, schema=EventCacheDataObject.SCHEMA)

    return events_df, cache_events_df


def set_event_load_testing_data(spark: SparkSession, config: ConfigParser):
    """
    Load input data (cell footprint and flagged events) and write to expected path.

    Args:
        spark (SparkSession): spark session.
        config (ConfigParser): component config.
    """
    # Cell footprint data
    input_cells_df = get_cellfootprint_testing_df(spark)
    test_data_path = config["Paths.Silver"]["cell_footprint_data_silver"]
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_cells_df
    input_data.write()

    # Cell to group data
    test_data_path = config["Paths.Silver"]["cell_to_group_data_silver"]
    input_cell_to_group_df = generate_input_cell_to_group_df(spark)
    input_data = SilverCellToGroupDataObject(spark, test_data_path)
    input_data.df = input_cell_to_group_df
    input_data.write()

    # # Group to tile data
    # test_data_path = config["Paths.Silver"]["group_to_tile_data_silver"]
    # input_group_to_tile_df = generate_input_group_to_tile_df(spark)
    # input_data = SilverGroupToTileDataObject(spark, test_data_path)
    # input_data.df = input_group_to_tile_df
    # input_data.write()

    # Get event data
    input_events_df, input_cache_df = get_event_load_testing_data(spark)

    # Event data
    test_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    input_data = SilverEventFlaggedDataObject(spark, test_data_path)
    input_data.df = input_events_df
    input_data.write()

    # Event cache data
    test_data_path = config["Paths.Silver"]["event_cache"]
    input_data = EventCacheDataObject(spark, test_data_path)
    input_data.df = input_cache_df
    input_data.write()
