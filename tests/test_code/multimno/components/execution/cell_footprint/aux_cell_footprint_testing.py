import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


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

    input_data_df = get_mno_network(spark)
    ### Write input data in test resources dir
    input_data = SilverNetworkDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write()


def get_mno_network(spark):

    TEST_NETWORK = [
        Row(
            cell_id="745788556413335",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="765058803725522",
            latitude=50.81049,
            longitude=4.43236,
            altitude=160.0,
            antenna_height=30.0,
            directionality=1,
            azimuth_angle=180.0,
            elevation_angle=5.0,
            horizontal_beam_width=65.0,
            vertical_beam_width=9.0,
            power=10.0,
            range=1000.0,
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
    return spark.createDataFrame(TEST_NETWORK, schema=SilverNetworkDataObject.SCHEMA)


EXPECTED_IMPUTE_DEFAULT_PROPERTIES = [
    Row(
        cell_id="745788556413335",
        latitude=50.8018,
        longitude=4.43345,
        altitude=150.0,
        antenna_height=30.0,
        directionality=0,
        azimuth_angle=None,
        elevation_angle=5.0,
        horizontal_beam_width=65.0,
        vertical_beam_width=9.0,
        power=2.0,
        range=5000.0,
        frequency=1281,
        technology="LTE",
        valid_date_start=datetime.strptime("2023-01-01 01:54:45", "%Y-%m-%d %H:%M:%S"),
        valid_date_end=datetime.strptime("2023-02-01 06:58:50", "%Y-%m-%d %H:%M:%S"),
        cell_type="default",
        year=2023,
        month=1,
        day=1,
        path_loss_exponent=3.75,
        azimuth_signal_strength_back_loss=-30,
        elevation_signal_strength_back_loss=-30,
    ),
    Row(
        cell_id="765058803725522",
        latitude=50.81049,
        longitude=4.43236,
        altitude=160.0,
        antenna_height=30.0,
        directionality=1,
        azimuth_angle=180.0,
        elevation_angle=5.0,
        horizontal_beam_width=65.0,
        vertical_beam_width=9.0,
        power=10.0,
        range=1000.0,
        frequency=252,
        technology="5G",
        valid_date_start=datetime.strptime("2023-01-01 01:54:45", "%Y-%m-%d %H:%M:%S"),
        valid_date_end=datetime.strptime("2023-02-01 06:58:50", "%Y-%m-%d %H:%M:%S"),
        cell_type="microcell",
        year=2023,
        month=1,
        day=1,
        path_loss_exponent=6.0,
        azimuth_signal_strength_back_loss=-30,
        elevation_signal_strength_back_loss=-30,
    ),
]
