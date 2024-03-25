"""
Contains the default threshold values used by the Network Syntactic Quality Warnings
"""

NETWORK_DEFAULT_THRESHOLDS = {
    # Size of the raw data, before cleaning
    "SIZE_RAW_DATA": {
        "OVER_AVERAGE": 30,
        "UNDER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": None,
        "ABS_VALUE_LOWER_LIMIT": None,
    },
    # Size of the clean data, after cleaning
    "SIZE_CLEAN_DATA": {
        "OVER_AVERAGE": 30,
        "UNDER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": None,
        "ABS_VALUE_LOWER_LIMIT": None,
    },
    # Rate of rows with any kind of error
    "TOTAL_ERROR_RATE": {
        "OVER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": 20,
    },
    # Rate of rows with null value, for each column
    "Missing_value_RATE": {
        "cell_id": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "valid_date_start": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "latitude": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "longitude": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "altitude": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "antenna_height": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "directionality": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "azimuth_angle": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "elevation_angle": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "horizontal_beam_width": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "vertical_beam_width": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "power": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "frequency": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "technology": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "cell_type": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
    },
    # Rate of rows with an out-of-range value, for each column
    "Out_of_range_RATE": {
        "cell_id": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "latitude": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "longitude": {
            "AVERAGE": 30,
            "VARIABILITY": 2,
            "ABS_VALUE_UPPER_LIMIT": 20,
        },
        "antenna_height": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "directionality": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "azimuth_angle": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "elevation_angle": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "horizontal_beam_width": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "vertical_beam_width": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "power": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "frequency": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "technology": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "cell_type": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        None: {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
    },
    # Rate of rows with a parsing error, for each column
    "Parsing_error_RATE": {
        "valid_date_start": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
        "valid_date_end": {
            "AVERAGE": 60,
            "VARIABILITY": 3,
            "ABS_VALUE_UPPER_LIMIT": 50,
        },
    },
}
