import configparser
import logging
from multimno.core.configuration import parse_configuration

from multimno.core.log import generate_logger
from tests.test_code.test_common import TEST_GENERAL_CONFIG_PATH


def test_log_generation():
    # Create a configparser object and set the log level to ERROR
    general_config_path = TEST_GENERAL_CONFIG_PATH
    config = parse_configuration(general_config_path)
    component_name = "test"
    # Generate the logger
    logger = generate_logger(config, component_name)
    logger.info("test test test test test")

    # Check if the logger is an instance of the logging.Logger class
    assert isinstance(logger, logging.Logger)
