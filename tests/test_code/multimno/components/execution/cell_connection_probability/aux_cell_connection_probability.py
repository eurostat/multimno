import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import pyspark.sql.functions as F

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
import multimno.core.utils as utils

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
            # valid_date_start=v_start,
            # valid_date_end=v_end,
            cell_connection_probability=1.0,
            posterior_probability=0.47058823529411764,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010533",
            grid_id="123231342131341",
            # valid_date_start=v_start,
            # valid_date_end=v_end,
            cell_connection_probability=0.25,
            posterior_probability=0.5294117647058824,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010512",
            grid_id="123231342131341",
            # valid_date_start=v_start,
            # valid_date_end=v_end,
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
            # valid_date_start=v_start,
            # valid_date_end=v_end,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010533",
            grid_id="123231342131341",
            # valid_date_start=v_start,
            # valid_date_end=v_end,
            signal_dominance=0.25,  # 0.451,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="956618596010512",
            grid_id="123231342131341",
            # valid_date_start=v_start,
            # valid_date_end=v_end,
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
    test_data_path = config["Paths.Silver"]["enriched_grid_data_silver"]

    # Prior is defined for two grids

    data = [
        Row(
            geometry="SRID=3035;POINT (3159450 2030350)",
            grid_id="123231342131342",
            elevation=129.12,
            prior_probability=0.2,
            environment_ple_coefficient=1,
            quadkey="1244312",
        ),
        Row(
            geometry="SRID=3035;POINT (3159550 2030350)",
            grid_id="123231342131341",
            elevation=125.12,
            prior_probability=0.9,
            environment_ple_coefficient=1,
            quadkey="1244312",
        ),
    ]
    input_data_df = spark.createDataFrame(data)
    input_data_df = input_data_df.withColumn("geometry", F.expr("ST_GeomFromEWKT(geometry)"))

    input_data_df = utils.apply_schema_casting(input_data_df, SilverEnrichedGridDataObject.SCHEMA)

    ### Write input data in test resources dir

    input_data = SilverEnrichedGridDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write()
