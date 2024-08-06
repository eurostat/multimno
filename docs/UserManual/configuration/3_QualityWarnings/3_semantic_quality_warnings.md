---
title: SemanticQualityWarnings Configuration
weight: 3
---

# SemanticQualityWarnings Configuration
To initialise and run the component two configs are used - `general_config.ini` and `event_semantic_quality_warnings.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
event_device_semantic_quality_metrics = ${Paths:silver_quality_metrics_dir}/semantic_quality_metrics
event_device_semantic_quality_warnings_log_table = ${Paths:silver_quality_warnings_dir}/semantic_quality_warnings_log_table
event_device_semantic_quality_warnings_bar_plot_data = ${Paths:silver_quality_warnings_dir}/semantic_quality_warnings_bar_plot_data
...
```

The expected parameters in `event_semantic_quality_warnings.ini` are as follows:
- **date_of_study**: string, format should be the one specified `date_format` (e.g., `2023-01-01` for `%Y-%m-%d`), the first date for which data will be processed by the component. All dates between this one and the specified in `data_period_end` will be processed (both inclusive).
- **date_format**: string, it indicates the format expected in `date_of_study`. For example, use `%Y-%m-%d` for the usual "2023-01-01" format separated by `-`.
- **thresholds**: dictionary, indicating the different thresholds to be used for raising warnings.

## Thresholds
The **thresholds** parameter is a dictionary of dictionaries, used to indicate the different thresholds and lookback periods to be used for each type of warning. In the case than one of the thresholds is missing, a default value will be used instead. The default values are contained in `multimno.core.constants.semantic_qw_default_thresholds.SEMANTIC_DEFAULT_THRESHOLDS`.

The dictionary structure is as follows:
 - `"CELL_ID_NON_EXISTENT"`: refers to events that make reference to a non-existent cell ID.
  - `"sd_lookback_days"`: integer, indicates the days to use as a lookback period to calculate the average and standard deviation over. By default, the value is 7.
  - `"min_sd"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from the average over the lookback period by more than its standard deviation multiplied by this threshold. By default, the value is 2.
  - `"min_percentage"`: integer or float, indicates the threshold for a warning to be raised for this error rate when the lookback date specified is strictly lower than 3. By default, the value is 0.
 - `"CELL_ID_NOT_VALID"`: refers to events that make reference to an existent cell ID, but the cell is not operative.
  - `"sd_lookback_days"`: integer, indicates the days to use as a lookback period to calculate the average and standard deviation over. By default, the value is 7.
  - `"min_sd"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from the average over the lookback period by more than its standard deviation multiplied by this threshold. By default, the value is 2.
  - `"min_percentage"`: integer or float, indicates the threshold for a warning to be raised for this error rate when the lookback date specified is strictly lower than 3. By default, the value is 0.
 - `"INCORRECT_EVENT_LOCATION"`: refers to events that have been flagged as having an incorrect location.
  - `"sd_lookback_days"`: integer, indicates the days to use as a lookback period to calculate the average and standard deviation over. By default, the value is 7.
  - `"min_sd"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from the average over the lookback period by more than its standard deviation multiplied by this threshold. By default, the value is 2.
  - `"min_percentage"`: integer or float, indicates the threshold for a warning to be raised for this error rate when the lookback date specified is strictly lower than 3. By default, the value is 0.
 - `"SUSPICIOUS_EVENT_LOCATION"`: refers to events that have been flagged as having a suspicious location.
  - `"sd_lookback_days"`: integer, indicates the days to use as a lookback period to calculate the average and standard deviation over. By default, the value is 7.
  - `"min_sd"`: integer or float, indicates the threshold for a warning to be raised when this error rate differs from the average over the lookback period by more than its standard deviation multiplied by this threshold. By default, the value is 2.
  - `"min_percentage"`: integer or float, indicates the threshold for a warning to be raised for this error rate when the lookback date specified is strictly lower than 3. By default, the value is 0.

# Configuration example

```ini
[Spark]
session_name = SemanticQualityWarnings

[SemanticQualityWarnings]

date_format = %Y-%m-%d
date_of_study = 2023-01-08

thresholds = {
    "CELL_ID_NON_EXISTENT": {
        "sd_lookback_days": 8,
        "min_sd": 1.5,
        "min_percentage": 5
    },
    "CELL_ID_NOT_VALID": {
        "sd_lookback_days": 8,
        "min_sd": 1.5,
        "min_percentage": 5
    },
    "INCORRECT_EVENT_LOCATION": {
        "sd_lookback_days": 8,
        "min_sd": 1.5,
        "min_percentage": 5
    },
    "SUSPICIOUS_EVENT_LOCATION": {
        "sd_lookback_days": 8,
        "min_sd": 1.5,
        "min_percentage": 5
    },
    }
```