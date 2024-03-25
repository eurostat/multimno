from pyspark.sql import SparkSession
from tests.test_code.fixtures import spark_session

# Dummy to avoid linting errors using pytest
fixtures = [spark_session]


def test_spark_session(spark_session):
    expected_conf_value = "org.apache.sedona.core.serde.SedonaKryoRegistrator"
    registrator_value = spark_session.conf.get("spark.kryo.registrator")

    assert isinstance(spark_session, SparkSession)
    assert expected_conf_value == registrator_value
