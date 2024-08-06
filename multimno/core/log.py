"""
Module that manages the logging functionality.
"""

import os
import logging

from configparser import ConfigParser
from datetime import datetime


class LoggerKeys:
    LOG_CONFIG_KEY = "Logging"
    CONSOLE_LOG_LEVEL = "level"
    FILE_LOG_LEVEL = "file_log_level"
    CONSOLE_FORMAT = "format"
    FILE_FORMAT = "file_format"
    DATEFMT = "datefmt"
    REPORT_PATH = "report_path"


def generate_logger(config: ConfigParser, component_id: str):
    """Function that initializes a logger.

    Args:
        config (ConfigParser): Object with the final configuration.

    Returns:
        (logging.Logger): Python logging object.
    """

    notset_level = logging.getLevelName(logging.NOTSET)

    # Parse config
    console_log_level = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.CONSOLE_LOG_LEVEL, fallback=None)
    file_log_level = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.FILE_LOG_LEVEL, fallback=None)
    console_format = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.CONSOLE_FORMAT, fallback=None)
    file_format = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.FILE_FORMAT, fallback=None)
    datefmt = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.DATEFMT, fallback=None)
    report_path = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.REPORT_PATH, fallback=None)

    # Check if logger already exists
    logger = logging.getLogger(component_id)
    if len(logger.handlers) > 0:
        logger.warning(f"Logger {component_id} already exists.")
        return logger

    # Define a console logger
    if console_log_level is not None and console_log_level != str(notset_level):
        # Set console handler
        console_h = logging.StreamHandler()
        console_h.setLevel(console_log_level)
        # Set console formatter
        console_formatter = logging.Formatter(fmt=console_format, datefmt=datefmt)
        console_h.setFormatter(console_formatter)
        # Add console handler to logger
        logger.addHandler(console_h)

    # Define a file logger
    if file_log_level is not None and file_log_level != str(notset_level):
        # Verify required fields for file logger
        if report_path is None:
            raise ValueError("report_path is required to build a file logger.")

        # Get log path
        today = datetime.now().strftime("%y%m%d")
        log_path = f"{report_path}/{component_id}/{component_id}_{today}.log"
        # Make report path + log dir
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # Set File handler
        file_h = logging.FileHandler(log_path)
        file_h.setLevel(file_log_level)
        # Set file formatter
        file_formatter = logging.Formatter(fmt=file_format, datefmt=datefmt)
        file_h.setFormatter(file_formatter)
        # Add file handler to logger
        logger.addHandler(file_h)

    # Set logger level
    logger.setLevel(logging.DEBUG)
    # Return logger
    return logger
