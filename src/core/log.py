import copy
import logging.config

from configparser import ConfigParser
from sys import stdout

from core.settings import APP_NAME

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
    __LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"verbose": {"format": (log_format), "datefmt": datefmt}},
        "handlers": None,
        "loggers": {},
    }

    __CONSOLE_HANDLER = {
        "level": "",
        "class": "logging.StreamHandler",
        "formatter": "verbose",
    }

    __LOGGER_TEMPLATE = {"level": "", "handlers": [], "propagate": False}

    # Create console handler
    log_config = copy.deepcopy(__LOGGING_CONFIG)
    console_handler = copy.deepcopy(__CONSOLE_HANDLER)
    console_handler["level"] = log_level
    console_handler["stream"] = stdout

    # Create a console handler in Warning level
    warning_level_console_handler = copy.deepcopy(__CONSOLE_HANDLER)
    warning_level_console_handler["level"] = "WARNING"

    # Add handlers to log config
    log_config["handlers"] = {"console": console_handler, "root_console": warning_level_console_handler}

    # Create application logger and add it to the python log config
    logger_template = copy.deepcopy(__LOGGER_TEMPLATE)
    logger_template["level"] = log_level
    logger_template["handlers"] = ["console"]
    log_config["loggers"][APP_NAME] = logger_template

    # Create root logger that all logging using dependency will use
    root_logger = copy.deepcopy(__LOGGER_TEMPLATE)
    root_logger["level"] = "DEBUG" if log_level == "DEBUG" else "WARNING"
    root_logger["handlers"] = ["root_console"]
    log_config["loggers"][""] = root_logger  # Make all loggers inherit config

    # Establish base logging
    logging.config.dictConfig(log_config)

    # Return application logger
    return logging.getLogger(APP_NAME)
