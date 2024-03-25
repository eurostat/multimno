from multimno.core.configuration import parse_configuration
from multimno.core.spark_session import SPARK_CONFIG_KEY

from tests.test_code.test_common import TEST_RESOURCES_PATH, TESTING_DATA_PATH, TEST_GENERAL_CONFIG_PATH


def test_parse_configuration():
    # Config/Setup
    general_config_path = TEST_GENERAL_CONFIG_PATH

    # Expected
    expected_value = f"{TESTING_DATA_PATH}/lakehouse/landing"

    # Execution
    config = parse_configuration(general_config_path)
    value = config.get("Paths", "landing_dir")
    config_dict = dict(config[SPARK_CONFIG_KEY])

    # Assert
    assert value == expected_value
    assert config_dict["spark.master"] == "local[*]"


def test_overwrite_config():
    # Config/Setup
    general_config_path = TEST_GENERAL_CONFIG_PATH
    path_component = f"{TEST_RESOURCES_PATH}/config/testing_spark.ini"

    # Expected
    expected_value = "TestingSparkSession"

    # Execution
    config = parse_configuration(general_config_path, path_component)
    value = config.get(SPARK_CONFIG_KEY, "session_name")

    # Assert
    assert value == expected_value
