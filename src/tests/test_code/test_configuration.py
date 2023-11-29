from core.configuration import parse_configuration
from core.spark_session import SPARK_CONFIG_KEY


def test_parse_configuration():
    path = "/opt/dev/src/tests/test_resources/general_config.ini"
    config = parse_configuration(path)

    value = config.get("Paths", "landing_dir")
    expected_value = "/opt/data/lakehouse/landing"
    assert value == expected_value

    config_dict = dict(config[SPARK_CONFIG_KEY])

    assert config_dict["spark.master"] == "local[*]"
