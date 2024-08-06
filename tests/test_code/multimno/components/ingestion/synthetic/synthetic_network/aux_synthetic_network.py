import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql import Row

from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject

from tests.test_code.multimno.components.ingestion.synthetic.synthetic_network.reference_data import BRONZE_NETWORK2
from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_data(spark):
    """
    Aux function to setup expected data using reference data file.

    Args:
        spark (SparkSession): spark session.
    """
    expected_df = spark.createDataFrame([Row(**el) for el in BRONZE_NETWORK2], BronzeNetworkDataObject.SCHEMA)
    return expected_df
