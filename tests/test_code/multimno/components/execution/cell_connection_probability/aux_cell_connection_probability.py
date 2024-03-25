import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql.functions import expr, col

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)

# For testing of geometry column schema for grid
from pyspark.testing import assertSchemaEqual
from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_cell_connection_probabilities(spark):
    date_format = "%Y-%m-%dT%H:%M:%S"
    v_start = datetime.strptime("2022-12-28T01:54:45", date_format)
    v_end = datetime.strptime("2022-12-30T06:58:50", date_format)

    expected_data = [
        Row(
            cell_id="956618596010533",
            grid_id="123231342131342",
            valid_date_start=v_start,
            valid_date_end=v_end,
            cell_connection_probability=1.0,
            posterior_probability=0.47058823529411764,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010533",
            grid_id="123231342131341",
            valid_date_start=v_start,
            valid_date_end=v_end,
            cell_connection_probability=0.25,
            posterior_probability=0.5294117647058824,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010512",
            grid_id="123231342131341",
            valid_date_start=v_start,
            valid_date_end=v_end,
            cell_connection_probability=0.75,
            posterior_probability=1.0,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverCellConnectionProbabilitiesDataObject.SCHEMA)
    return expected_data_df


def set_input_cell_footprint_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    date_format = "%Y-%m-%dT%H:%M:%S"
    v_start = datetime.strptime("2022-12-28T01:54:45", date_format)
    v_end = datetime.strptime("2022-12-30T06:58:50", date_format)

    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["cell_footprint_data_silver"]

    ### # rows: for a two cells, so that one cell has the same grid id
    # as ain one of the other two rows
    data = [
        Row(
            cell_id="956618596010533",
            grid_id="123231342131342",
            valid_date_start=v_start,
            valid_date_end=v_end,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010533",
            grid_id="123231342131341",
            valid_date_start=v_start,
            valid_date_end=v_end,
            signal_dominance=0.25,  # 0.451,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010512",
            grid_id="123231342131341",
            valid_date_start=v_start,
            valid_date_end=v_end,
            signal_dominance=0.75,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    input_data_df = spark.createDataFrame(data, schema=SilverCellFootprintDataObject.SCHEMA)

    ### Write input data in test resources dir
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)


def set_input_grid_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    # Currently no grid partitioning columns
    # partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["grid_data_silver"]

    # Prior is defined for two grids

    data = [
        Row(
            geometry="POINT (-12.6298 33.8781)",
            grid_id="123231342131342",
            elevation=129.12,
            land_use="urban",
            prior_probability=0.2,
        ),
        Row(
            geometry="POINT (-12.6292 33.8783)",
            grid_id="123231342131341",
            elevation=125.12,
            land_use="urban",
            prior_probability=0.9,
        ),
    ]
    input_data_df_starting = spark.createDataFrame(data)
    input_data_df_starting = input_data_df_starting.withColumn("geometry_col", expr("ST_GeomFromWKT(geometry)"))
    input_data_df_starting = input_data_df_starting.withColumn("geometry", col("geometry_col")).drop("geometry_col")
    input_data_df_starting = input_data_df_starting.withColumn("elevation", col("elevation").cast("float"))
    input_data_df_starting = input_data_df_starting.withColumn(
        "prior_probability", col("prior_probability").cast("float")
    )

    input_data_df = input_data_df_starting[
        "geometry",
        "grid_id",
        "elevation",
        "land_use",
        "prior_probability",
    ]

    assertSchemaEqual(input_data_df.schema, SilverGridDataObject.SCHEMA)

    ### Write input data in test resources dir

    input_data = SilverGridDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write()
