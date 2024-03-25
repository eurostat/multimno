---
title: NetworkQualityWarnings Configuration
weight: 12
---

# NetworkQualityWarnings Configuration
To initialise and run the component two configs are used - `general_config.ini` and `network_quality_warnings.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
network_syntactic_quality_metrics_by_column = ${Paths:silver_quality_metrics_dir}/network_syntactic_quality_metrics_by_column
network_syntactic_quality_warnings_log_table = ${Paths:silver_quality_warnings_dir}/network_syntactic_quality_warnings_log_table
network_syntactic_quality_warnings_line_plot_data = ${Paths:silver_quality_warnings_dir}/network_syntactic_quality_warnings_line_plot_data
network_syntactic_quality_warnings_pie_plot_data = ${Paths:silver_quality_warnings_dir}/network_syntactic_quality_warnings_pie_plot_data
...
```

The expected parameters in `network_quality_warnings.ini` are as follows:
- **date_of_study**: string, format should be the one specified `date_format` (e.g., `2023-01-01` for `%Y-%m-%d`), the first date for which data will be processed by the component. All dates between this one and the specified in `data_period_end` will be processed (both inclusive).
- **date_format**: string, it indicates the format expected in `date_of_study`. For example, use `%Y-%m-%d` for the usual "2023-01-01" format separated by `-`.
- **lookback_period**: string, it indicates the length of the lookback period used to compare the metrics of the date of study with past data volume and error rates. Three possible values are accepted: `week`, `month`, and `quarter`.
- **thresholds**: dictionary, indicating the different thresholds to be used for raising warnings.

## Thresholds
The **thresholds** parameter is a dictionary of dictionaries, used to indicate the different thresholds to be used for each type of warning. In the case that one of the thresholds is missing, a default value will be used instead. The default values are contained in `multimno.core.constants.network_default_thresholds.NETWORK_DEFAULT_THRESHOLDS`.

Ihe dictionary structure is as follows:
- `"SIZE_RAW_DATA"`: refers to the size of the input data.
  - `"OVER_AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the input raw file is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30`.
  - `"UNDER_AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the input raw file is lower than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30`.
  - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the input raw file differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, either over or under the average. By default, the value is `2`.
  - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the input raw file is greater than this value. By default, it is equal to the threshold calculated from the `VARIABILITY` parameter over the average.
  - `"ABS_VALUE_LOWER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the input raw file is greater than this value. By default, it is equal to the threshold calculated from the `VARIABILITY` parameter under the average.

- `"SIZE_CLEAN_DATA"`: refers to the size of the output data.
  - `"OVER_AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the output processed file is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30`.
  - `"UNDER_AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the output processed file is lower than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30`.
  - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the output processed file differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, either over or under the average. By default, the value is `2`.
  - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the output processed file is greater than this value. by default, it is equal to the threshold calculated from the `VARIABILITY` parameter over the average.
  - `"ABS_VALUE_LOWER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when the number of rows in the output processed file is greater than this value. By default, it is equal to the threshold calculated from the `VARIABILITY` parameter under the average.

- `"TOTAL_ERROR_RATE"`: refers to the percentage of rows preserved from the input file, i.e., the rows that passed the cleaning/check procedure.
  - `"OVER_AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when the error rate is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30`.
  - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when the error rate differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, obly over the average. By default, the value is `2`.
  - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when the error rate is greater than this value. By default, the value is `20`.

- `"Missing_value_RATE"`: refers to the percentage of missing/null values of a given field in the input data. The value of this dictionary key is a dictionary where the keys are field names, and the values are a dictionary containing the thresholds form this error type and field.
  - Admitted keys (i.e., fields) are: `cell_id`, `valid_date_start`, `latitude`, `longitude`, `altitude`, `antenna_height`, `directionality`, `azimuth_angle`, `elevation_angle`, `horizontal_beam_width`, `vertical_beam_width`, `power`, `frequency`, `technology`, and `cell_type`.
  - Each key (i.e., field) has the following thresholds:
    - `"AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `30` for `cell_id`, `latitude`, and `longitude`, and `60` otherwise.
    - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, obly over the average. By default, the value is `2` for `cell_id`, `latitude`, and `longitude`, and `3` otherwise.
    - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this value. By default, the value is `20` for `cell_id`, `latitude`, and `longitude`, and `50` otherwise.

- `"Out_of_range_RATE"`: refers to the percentage of out of bounds, out of range or invalid values of a given field in the input data. The value of this dictionary key is a dictionary where the keys are field names, and the values are a dictionary containing the thresholds form this error type and field.
  - Admitted keys (i.e., fields) are: `"cell_id"`, `"latitude"`, `"longitude"`, `"antenna_height"`, `"directionality"`, `"azimuth_angle"`, `"elevation_angle"`, `"horizontal_beam_width"`, `"vertical_beam_width"`, `"power"`, `"frequency"`, `"technology"`, and `"cell_type"`. Exceptionally, the `None` value is also accepted, referring to the specific error where `valid_date_end` is a point int time earlier than `valid_date_start`.
  - Each key has the following thresholds:
    - `"AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `20` for `cell_id`, `latitude`, and `longitude`, and `60` otherwise.
    - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, obly over the average. By default, the value is `2` for `cell_id`, `latitude`, and `longitude`, and `3` otherwise.
    - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this value. By default, the value is `20` for `cell_id`, `latitude`, and `longitude`, and `50` otherwise.

- `"Parsing_error_RATE"`: refers to values that could not be parsed.
  - `"valid_date_start"`:
    - `"AVERAGE"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this threshold, in percentage different, when compared to the average over the lookback period. By default, the value is `60`.
    - `"VARIABILITY"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from its average over the lookback period by more than its standard deviation multiplied by this threshold. This is, this value is a threshold of the number of standard deviations than a value can differ from its average over the lookback period, obly over the average. By default, the value is `3`.
    - `"ABS_VALUE_UPPER_LIMIT"`: integer or float, indicates the threshold for a warning to be raised when this error rate is greater than this value. By default, the value is `50`.

# Configuration example

```ini
[Spark]
session_name = NetworkQualityWarnings

[NetworkQualityWarnings]
date_format = %Y-%m-%d
date_of_study = 2023-01-08
lookback_period = week

# All values must be numeric
# Missing parameter will take the default value
# Incorrect value will throw an error
thresholds = {
    "SIZE_RAW_DATA": {
        "OVER_AVERAGE": 30,
        "UNDER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": None,
        "ABS_VALUE_LOWER_LIMIT": None,
    },
    "SIZE_CLEAN_DATA": {
        "OVER_AVERAGE": 30,
        "UNDER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": None,
        "ABS_VALUE_LOWER_LIMIT": None,
    },
    "TOTAL_ERROR_RATE": {
        "OVER_AVERAGE": 30,
        "VARIABILITY": 2,
        "ABS_VALUE_UPPER_LIMIT": 20,
    },
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
        }
    }
    }
```