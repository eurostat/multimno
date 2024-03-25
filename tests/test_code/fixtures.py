import pytest
from multimno.core.configuration import parse_configuration
from multimno.core.spark_session import generate_spark_session

from tests.test_code.test_common import TEST_GENERAL_CONFIG_PATH
import logging


@pytest.fixture(scope="session")
def spark_session(request):
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)
    spark = generate_spark_session(config)

    def teardown():
        spark.stop()

    logging.getLogger("py4j").setLevel(logging.ERROR)

    request.addfinalizer(teardown)
    return spark
