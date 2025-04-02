import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Row
from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import SilverEnrichedGridDataObject
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)


from tests.test_code.multimno.components.execution.usual_environment_aggregation.reference_data import (
    ENRICHED_GRID_DATA,
    USUAL_ENVIRONMENT_LABELS_DATA,
    AGGREGATED_USUAL_ENVIRONMENT_DATA,
    AGGREGATED_USUAL_ENVIRONMENT_DATA_UNIFORM,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


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
    expected_df = spark.createDataFrame(
        [Row(**el) for el in AGGREGATED_USUAL_ENVIRONMENT_DATA], SilverAggregatedUsualEnvironmentsDataObject.SCHEMA
    )
    # expected_df = cast_user_id_to_binary(expected_df)
    # expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_initial_time)
    # expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_end_time)
    expected_df = spark.createDataFrame(expected_df.rdd, SilverAggregatedUsualEnvironmentsDataObject.SCHEMA)
    return expected_df


@pytest.fixture
def expected_data_uniform(spark):
    """
    Aux function to setup expected data using reference data file (considering uniform tile weights).

    Args:
        spark (SparkSession): spark session.
    """
    expected_df = spark.createDataFrame(
        [Row(**el) for el in AGGREGATED_USUAL_ENVIRONMENT_DATA_UNIFORM],
        SilverAggregatedUsualEnvironmentsDataObject.SCHEMA,
    )
    # expected_df = cast_user_id_to_binary(expected_df)
    # expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_initial_time)
    # expected_df = cast_timestamp_field(expected_df, ColNames.time_slot_end_time)
    expected_df = spark.createDataFrame(expected_df.rdd, SilverAggregatedUsualEnvironmentsDataObject.SCHEMA)
    return expected_df


def set_input_data(spark: SparkSession, config: ConfigParser):
    """
    Load input data (enriched cell footprint and ue labels) and write to expected path.

    Args:
        spark (SparkSession): spark session.
        config (ConfigParser): component config.
    """
    print(SilverEnrichedGridDataObject.SCHEMA)
    print(SilverUsualEnvironmentLabelsDataObject.SCHEMA)

    # INPUT 1: Enriched Grid
    enriched_grid_data_path = config["Paths.Silver"]["enriched_grid_data_silver"]
    input_data = SilverEnrichedGridDataObject(spark, enriched_grid_data_path)
    enriched_grid_df = spark.createDataFrame(
        [Row(**el) for el in ENRICHED_GRID_DATA], SilverEnrichedGridDataObject.SCHEMA
    )
    input_data.df = enriched_grid_df
    input_data.write()

    # INPUT 2: Usual Environment Labels
    ue_labels_data_path = config["Paths.Silver"]["usual_environment_labels_data_silver"]
    input_data = SilverUsualEnvironmentLabelsDataObject(spark, ue_labels_data_path)
    usual_environment_labels_df = spark.createDataFrame(
        [Row(**el) for el in USUAL_ENVIRONMENT_LABELS_DATA], SilverUsualEnvironmentLabelsDataObject.SCHEMA
    )
    input_data.df = usual_environment_labels_df
    input_data.write()

    """input_events_df = cast_timestamp_field(
        cast_user_id_to_binary(spark.createDataFrame([Row(**el) for el in EVENTS], EVENTS_AUX_SCHEMA)),
        ColNames.timestamp,
    )"""


def set_input_data_uniform(spark: SparkSession, config: ConfigParser):
    """
    Load input data (enriched cell footprint and ue labels) and write to expected path.

    Args:
        spark (SparkSession): spark session.
        config (ConfigParser): component config.
    """
    print(SilverEnrichedGridDataObject.SCHEMA)
    print(SilverUsualEnvironmentLabelsDataObject.SCHEMA)

    # INPUT 1: Grid
    grid_data_path = config["Paths.Silver"]["grid_data_silver"]
    input_data = SilverGridDataObject(spark, grid_data_path)
    GRID_DATA = [
        {k: v for k, v in el.items() if k in [ColNames.grid_id, ColNames.geometry, ColNames.origin, ColNames.quadkey]}
        for el in ENRICHED_GRID_DATA
    ]
    grid_df = spark.createDataFrame([Row(**el) for el in GRID_DATA], SilverGridDataObject.SCHEMA)
    input_data.df = grid_df
    input_data.write()

    # INPUT 2: Usual Environment Labels
    ue_labels_data_path = config["Paths.Silver"]["usual_environment_labels_data_silver"]
    input_data = SilverUsualEnvironmentLabelsDataObject(spark, ue_labels_data_path)
    usual_environment_labels_df = spark.createDataFrame(
        [Row(**el) for el in USUAL_ENVIRONMENT_LABELS_DATA], SilverUsualEnvironmentLabelsDataObject.SCHEMA
    )
    input_data.df = usual_environment_labels_df
    input_data.write()
