import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_net(spark):
    date_format = "%Y-%m-%dT%H:%M:%S"
    v_start = datetime.strptime("2022-12-28T01:54:45", date_format)
    v_end = datetime.strptime("2024-12-30T06:58:50", date_format)

    # expected_data = data[:1]
    expected_data = [
        Row(
            cell_id="956618596010533",
            latitude=40.46147918701172,
            longitude=-3.735163450241089,
            altitude=4969.04931640625,
            antenna_height=106.51593017578125,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=42.29446029663086,
            horizontal_beam_width=39.793006896972656,
            vertical_beam_width=100.03053283691406,
            power=357.10943603515625,
            range=10_000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=v_start,
            valid_date_end=v_end,
            cell_type="microcell",
            year=2023,
            month=1,
            day=1,
        )
    ]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverNetworkDataObject.SCHEMA)
    return expected_data_df


def set_input_network_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Bronze"]["network_data_bronze"]
    ### 2 rows: One correct, One with cell_id to NULL. Expected one row after cleaning
    data = [
        Row(
            cell_id="956618596010533",
            latitude=40.46147918701172,
            longitude=-3.735163450241089,
            altitude=4969.04931640625,
            antenna_height=106.51593017578125,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=42.29446029663086,
            horizontal_beam_width=39.793006896972656,
            vertical_beam_width=100.03053283691406,
            power=357.10943603515625,
            range=10000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start="2022-12-28T01:54:45",
            valid_date_end="2024-12-30T06:58:50",
            cell_type="microcell",
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id=None,
            latitude=40.4393310546875,
            longitude=-3.585763454437256,
            altitude=918.4267578125,
            antenna_height=114.41542053222656,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=-82.24849700927734,
            horizontal_beam_width=65.6939926147461,
            vertical_beam_width=323.4800109863281,
            power=130.9789581298828,
            range=5_000.0,
            frequency=252,
            technology="5G",
            valid_date_start="2023-12-21T09:27:57",
            valid_date_end=None,
            cell_type="picocell",
            year=2023,
            month=1,
            day=1,
        ),
    ]

    input_data_df = spark.createDataFrame(data, schema=BronzeNetworkDataObject.SCHEMA)
    ### Write input data in test resources dir
    input_data = BronzeNetworkDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)
