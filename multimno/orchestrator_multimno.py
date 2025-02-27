#!/usr/bin/python

"""
Module that orchestrates MultiMNO pipeline components. A spark-submit will be performed for each 
component in the pipeline.

Usage: 

```
python multimno/orchestrator.py <pipeline.json>
```

- pipeline.json: Path to a json file with the pipeline configuration.
"""

import argparse
from datetime import datetime
import json
import os
import subprocess
import sys
import re
import logging

from multimno.core.log import LoggerKeys
from multimno.core.configuration import parse_configuration
from multimno.core.settings import QUALITY_WARNINGS_EXIT_CODE, SUCCESS_EXIT_CODE


def create_logger(general_config_path: str):
    """
    Create a logger with the specified configuration.

    Args:
        general_config_path (str): The path to the general configuration file.

    Returns:
        logging.Logger: The created logger object.
    """
    # Get log configuration
    config = parse_configuration(general_config_path)

    report_path = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.REPORT_PATH, fallback=None)
    file_format = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.FILE_FORMAT, fallback=None)
    datefmt = config.get(LoggerKeys.LOG_CONFIG_KEY, LoggerKeys.DATEFMT, fallback=None)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)  # Set the logging level

    # Get log path
    today = datetime.now().strftime("%y%m%d_%H%M%S")
    log_path = f"{report_path}/multimno_{today}.log"
    # Make report path + log dir
    os.makedirs(os.path.dirname(log_path), exist_ok=True)

    # Create a file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.DEBUG)

    # Create a console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter(fmt=file_format, datefmt=datefmt)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def main():
    # ----------- ARGS ------------
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Entrypoint of multimno orchestrator for launching sequentially a pipeline of components."
        + "\nEach component will be launched in a spark-submit command."
        + "Reference execution documentation: https://eurostat.github.io/multimno/latest/UserManual/execution"
    )
    # Add the arguments
    parser.add_argument("pipeline_config_path", help="The path to the pipeline configuration file.")
    # Parse the arguments
    args = parser.parse_args()

    pipeline_config_path = args.pipeline_config_path

    # ----------- MAIN ENTRYPOINT ------------
    # Main should always be at same level as the orchestrator script
    script_location = os.path.dirname(os.path.abspath(__file__))
    ENTRYPOINT = f"{script_location}/main_multimno.py"

    # ----------- LOAD CONFIGURATION ------------
    # Verify configuration files
    if not os.path.exists(pipeline_config_path):
        print(f"Pipeline config path not found: {pipeline_config_path}", file=sys.stderr)
        sys.exit(1)

    # Load pipeline
    try:
        with open(pipeline_config_path, encoding="utf-8") as out:
            pipeline_config = json.load(out)
    except json.decoder.JSONDecodeError:
        print(f"Pipeline JSON file could not be decoded.\nBad formatted file: {pipeline_config_path}.", file=sys.stderr)
        sys.exit(1)

    # Load general config
    general_config_path = pipeline_config["general_config_path"]

    # Load spark submit args
    spark_args = pipeline_config["spark_submit_args"]
    # Check for special characters
    SPECIAL_CHARS = r"[\`~!@#$%^&*()\+{}\[\]|;'\"<>?]"
    for s in spark_args:
        if re.search(SPECIAL_CHARS, s):
            print(f"Spark submit argument contains special characters: {s}", file=sys.stderr)
            sys.exit(1)

    spark_submit_command_base = ["spark-submit"] + spark_args

    # Create logger
    logger = create_logger(general_config_path)

    # ----------- PIPELINE ------------
    # Start pipeline
    logger.info("Starting pipeline...")

    for step in pipeline_config["pipeline"]:
        component_id = step["component_id"]
        component_config_path = step["component_config_path"]
        logger.info(f"Launching component: {component_id}")

        # Set spark submit command
        spark_submit_suffix = [ENTRYPOINT, component_id, general_config_path, component_config_path]
        spark_submit_command = spark_submit_command_base + spark_submit_suffix

        # Launch command
        result = subprocess.run(spark_submit_command, check=False)

        # Parse result

        if result.returncode == QUALITY_WARNINGS_EXIT_CODE:
            logger.error(f"Component {component_id} raised a critical quality warnings exit.")
            sys.exit(QUALITY_WARNINGS_EXIT_CODE)
        if result.returncode != SUCCESS_EXIT_CODE:
            logger.error(
                "[X] ------ Component Error ------\n" + f"Error executing component: {component_id}\n"
                f"General config: {general_config_path}\n"
                f"Component config: {component_config_path}\n"
                "[X] -----------------------------\n"
            )
            sys.exit(1)
        logger.info(f"Component {component_id} finished successfully.")

    logger.info("Pipeline finished successfully!")


if __name__ == "__main__":
    main()
