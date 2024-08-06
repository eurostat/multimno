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
)

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_event_flagged_data_object import SilverEventFlaggedDataObject
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)

from tests.test_code.multimno.components.execution.daily_permanence_score.reference_data import (
    EVENTS,
    CELL_FOOTPRINT,
    DPS,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


EVENTS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.timestamp, StringType(), nullable=False),
        StructField(ColNames.mcc, IntegerType(), nullable=False),
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

DPS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.grid_id, StringType(), nullable=False),
        StructField(ColNames.time_slot_initial_time, StringType(), nullable=False),
        StructField(ColNames.time_slot_end_time, StringType(), nullable=False),
        StructField(ColNames.dps, IntegerType(), nullable=False),
        StructField(ColNames.year, ShortType(), nullable=False),
        StructField(ColNames.month, ByteType(), nullable=False),
        StructField(ColNames.day, ByteType(), nullable=False),
        StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        StructField(ColNames.id_type, StringType(), nullable=False),
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
def expected_data(spark):
    """
    Aux function to setup expected data using reference data file.

    Args:
        spark (SparkSession): spark session.
    """
    expected_df = spark.createDataFrame([Row(**el) for el in DPS], DPS_AUX_SCHEMA)
    expected_df = cast_user_id_to_binary(expected_df)
    expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_initial_time)
    expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_end_time)
    expected_df = spark.createDataFrame(expected_df.rdd, SilverDailyPermanenceScoreDataObject.SCHEMA)
    return expected_df


def set_input_data(spark: SparkSession, config: ConfigParser):
    """
    Load input data (cell footprint and flagged events) and write to expected path.

    Args:
        spark (SparkSession): spark session.
        config (ConfigParser): component config.
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["cell_footprint_data_silver"]
    input_cells_df = spark.createDataFrame([Row(**el) for el in CELL_FOOTPRINT], SilverCellFootprintDataObject.SCHEMA)
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_cells_df
    input_data.write(partition_columns=partition_columns)

    partition_columns = [ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo]
    test_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    input_events_df = cast_timestamp_field(
        cast_user_id_to_binary(spark.createDataFrame([Row(**el) for el in EVENTS], EVENTS_AUX_SCHEMA)),
        ColNames.timestamp,
    )
    input_data = SilverEventFlaggedDataObject(spark, test_data_path)
    input_data.df = input_events_df
    input_data.write(partition_columns=partition_columns)
