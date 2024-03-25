import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql import functions as F

from sedona.sql import st_constructors as STC

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_signal_strength_data_object import SilverSignalStrengthDataObject
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject

from tests.test_code.multimno.components.execution.signal_strength.reference_data import SIGNAL_STRENGTH, REFERENCE_GRID
from tests.test_code.test_common import TEST_GENERAL_CONFIG_PATH

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_data(spark):
    """
    Aux function to setup expected data using reference data file

    Args:
        spark (SparkSession): spark session
    """
    expected_ss_sdf = spark.createDataFrame(SIGNAL_STRENGTH)
    expected_ss_sdf = (
        expected_ss_sdf.withColumn(ColNames.valid_date_start, F.lit("2023-01-01"))
        .withColumn(ColNames.valid_date_end, F.lit("2023-01-02"))
        .withColumn(ColNames.year, F.lit(2023))
        .withColumn(ColNames.month, F.lit(1))
        .withColumn(ColNames.day, F.lit(1))
    )

    expected_ss_sdf = expected_ss_sdf.select(*[field.name for field in SilverSignalStrengthDataObject.SCHEMA.fields])
    columns = {
        field.name: F.col(field.name).cast(field.dataType) for field in SilverSignalStrengthDataObject.SCHEMA.fields
    }
    expected_ss_sdf = expected_ss_sdf.withColumns(columns)

    return expected_ss_sdf


def set_input_network_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["network_data_silver"]
    ### 2 rows: One correct, One with cell_id to NULL. Expected one row after cleaning
    input_network = [
        Row(
            cell_id="745788556413335",
            latitude=40.443302,
            longitude=-3.626978,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type="picocell",
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="765058803725522",
            latitude=40.402412,
            longitude=-3.723693,
            altitude=160.0,
            antenna_height=30.0,
            directionality=1,
            azimuth_angle=270.0,
            elevation_angle=5.0,
            horizontal_beam_width=65.0,
            vertical_beam_width=9.0,
            power=10.0,
            frequency=252,
            technology="5G",
            valid_date_start=datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type="microcell",
            year=2023,
            month=1,
            day=1,
        ),
    ]

    input_data_df = spark.createDataFrame(input_network, schema=SilverNetworkDataObject.SCHEMA)
    ### Write input data in test resources dir
    input_data = SilverNetworkDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)


def set_input_grid_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data using reference data file

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    test_data_path = config["Paths.Silver"]["grid_data_silver"]

    input_grid_df = spark.createDataFrame(REFERENCE_GRID)
    input_grid_df = input_grid_df.withColumn(ColNames.geometry, STC.ST_GeomFromEWKT(ColNames.geometry))

    input_data = SilverGridDataObject(spark, test_data_path)
    input_data.df = input_grid_df
    input_data.write()
