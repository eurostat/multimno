import os
from configparser import ConfigParser, ExtendedInterpolation


def parse_configuration(general_config_path: str, component_config_path: str = ""):
    """Function that parses a list of configurations in a single ConfigParser object. It expects 
    the first element of the list to be the path to general configuration path. It will override 
    values of the general configuration file with component configuration data.

    Args:
        config_paths (list): List of paths. First Element is the general configuration path.

    Raises:
        FileNotFoundError: If the general configuration path is doesn't exist

    Returns:
        ConfigParser: ConfigParser object with some general configuration values overriden by 
    """

    # Check general configuration file
    if not os.path.exists(general_config_path):
        raise FileNotFoundError(
            f"General Config file Not found: {general_config_path}")

    config_paths = [general_config_path, component_config_path]

    converters = {
        "list": lambda val: [i.strip() for i in val.strip().split("\n")],
        "eval": eval,
    }

    parser: ConfigParser = ConfigParser(converters=converters, interpolation=ExtendedInterpolation(), inline_comment_prefixes="#")
    parser.read(config_paths)

    return parser


