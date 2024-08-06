import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime, date
from hashlib import sha256

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_event_data_object import (
    SilverEventDataObject,
)
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)
from multimno.core.data_objects.silver.silver_device_activity_statistics import (
    SilverDeviceActivityStatistics,
)


from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture(scope="module")
def expected_device_activity_statistics(spark):
    expected_data = [[sha256(b"1").digest(), 2023, 1, 1, 1, 1, 1, None, 1, None, None]]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverDeviceActivityStatistics.SCHEMA)
    return expected_data_df


def write_input_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    date_format = "%Y-%m-%d %H:%M:%S"
    test_events_path = config["Paths.Silver"]["event_data_silver"]
    test_topology_path = config["Paths.Silver"]["network_data_silver"]
    event_data = [
        [
            sha256(b"1").digest(),
            datetime.strptime("2023-01-01 13:00:00", date_format),
            123,
            "01",
            None,
            "0",
            None,
            None,
            None,
            2023,
            1,
            1,
            0,
        ]
    ]

    topology_data = [["0", 0.0, 0.0, 2023, 1, 1]]
    # Add null columns for optional values
    topology_data = [t[:3] + [1.0, 1.0, 1] + [None] * 11 + t[-3:] for t in topology_data]

    events_data_df = spark.createDataFrame(data=event_data, schema=SilverEventDataObject.SCHEMA)
    topology_data_df = spark.createDataFrame(data=topology_data, schema=SilverNetworkDataObject.SCHEMA)

    ### Write input data in test resources dir
    event_do = SilverNetworkDataObject(spark, test_events_path)
    event_do.df = events_data_df
    event_do.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])

    topology_do = SilverNetworkDataObject(spark, test_topology_path)
    topology_do.df = topology_data_df
    topology_do.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])
