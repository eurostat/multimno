"""
Module that manages the application configuration.
"""

import os
from configparser import ConfigParser, ExtendedInterpolation


def parse_configuration(general_config_path: str, component_config_path: str = "") -> ConfigParser:
    """Function that parses a list of configurations in a single ConfigParser object. It expects
    the first element of the list to be the path to general configuration path. It will override
    values of the general configuration file with component configuration data.

    Args:
        general_config_path (list): Path to the general configuration file.
        component_config_path (str): Path to the component configuration file.

    Raises:
        FileNotFoundError: If the general configuration path is doesn't exist

    Returns:
        config: ConfigParser object with the configuration data.
    """

    # Check general configuration file
    if not os.path.exists(general_config_path):
        raise FileNotFoundError(f"General Config file Not found: {general_config_path}")

    config_paths = [general_config_path, component_config_path]

    converters = {
        "list": lambda val: [i.strip() for i in val.strip().split("\n")],
        "eval": eval,
    }

    parser: ConfigParser = ConfigParser(
        converters=converters, interpolation=ExtendedInterpolation(), inline_comment_prefixes="#"
    )
    parser.optionxform = str
    parser.read(config_paths)

    return parser
