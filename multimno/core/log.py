"""
Module that manages the logging functionality.
"""
import copy
import logging.config

from configparser import ConfigParser
from sys import stdout

from multimno.core.settings import APP_NAME

LOG_CONFIG_KEY = "Logging"


def generate_logger(config: ConfigParser):
    """Function that initializes a logger.

    Args:
        config (ConfigParser): Object with the final configuration.

    Returns:
        Logger: Python logging object.
    """
    # Parse config
    log_level = config.get(LOG_CONFIG_KEY, "level")
    log_format = config.get(LOG_CONFIG_KEY, "format")
    datefmt = config.get(LOG_CONFIG_KEY, "datefmt")

    # Establish python log config templates
    loggin_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"verbose": {"format": (log_format), "datefmt": datefmt}},
        "handlers": None,
        "loggers": {},
    }

    console_handler_config = {
        "level": "",
        "class": "logging.StreamHandler",
        "formatter": "verbose",
    }

    logger_template_config = {"level": "", "handlers": [], "propagate": False}

    # Create console handler
    log_config = copy.deepcopy(loggin_config)
    console_handler = copy.deepcopy(console_handler_config)
    console_handler["level"] = log_level
    console_handler["stream"] = stdout

    # Create a console handler in Warning level
    warning_level_console_handler = copy.deepcopy(console_handler_config)
    warning_level_console_handler["level"] = "WARNING"

    # Add handlers to log config
    log_config["handlers"] = {"console": console_handler, "root_console": warning_level_console_handler}

    # Create application logger and add it to the python log config
    logger_template = copy.deepcopy(logger_template_config)
    logger_template["level"] = log_level
    logger_template["handlers"] = ["console"]
    log_config["loggers"][APP_NAME] = logger_template

    # Create root logger that all logging using dependency will use
    root_logger = copy.deepcopy(logger_template_config)
    root_logger["level"] = "DEBUG" if log_level == "DEBUG" else "WARNING"
    root_logger["handlers"] = ["root_console"]
    log_config["loggers"][""] = root_logger  # Make all loggers inherit config

    # Establish base logging
    logging.config.dictConfig(log_config)

    # Return application logger
    return logging.getLogger(APP_NAME)
