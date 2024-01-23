import pytest
from multimno.core.configuration import parse_configuration
from multimno.core.spark_session import generate_spark_session
from pyspark.sql import SparkSession


@pytest.fixture
def spark_fixture():
    config_path = "/opt/dev/tests/test_resources/testing_spark.ini"
    config = parse_configuration(config_path)
    spark = generate_spark_session(config)
    yield spark
