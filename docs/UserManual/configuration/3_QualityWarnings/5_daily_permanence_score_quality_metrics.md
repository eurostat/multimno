---
title: DailyPermanenceScoreQualityMetrics Configuration
weight: 5
---

# DailyPermanenceScoreQualityMetrics Configuration
To initialise and run the component two configs are used - `general_config.ini` and `daily_permanence_score_quality_metrics.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
daily_permanence_score_data_silver = ${Paths:ue_dir}/daily_permanence_score
daily_permanence_score_quality_metrics = ${Paths:silver_quality_metrics_dir}/daily_permanence_score_quality_metrics
...
```

The expected parameters in `daily_permanence_score_quality_metrics.ini` are as follows:
 - **clear_destination_directory**: bool, whether to clear the quality metrics directory before running the component. Example: `False`.
 - **data_period_start**: string, with `YYYY-MM-DD` format, indicating the first date of the date interval for which the daily permanence score quality metrics will be computed. All days between **data_period_start** and **data_period_end**, both inclusive, will be processed individually. Example: `2023-01-01`.
 - **data_period_end**: string, with `YYYY-MM-DD` format, indicating the last date of the date interval for which the daily permanence score quality metrics will be computed. All days between **data_period_start** and **data_period_end**, both inclusive, will be processed individually. Example: `2023-01-03`.
 - **unknown_intervals_pct_threshold**: float between `0.0` and `100.0`, it indicates the percentage threshold of "unknown" time slots that a device must have in order to be flagged for the warning of too many devices with a high number of "unknown" time slots. Devices with a percentage of their time slots classified as "unknown" equal or higher than this threshold will be flagged. Example: `50`.
 - **non_crit_unknown_devices_pct_threshold**: float between `0.0` and `100.0`, it indicates the non-critical warning threshold for high number of devices with "unknown" time slots. A non-critical warning will be raised if the percentage of these devices with respect to the total number of devices in this date's daily permanence score data is strictly higher than this threshold, and strictly lower than **crit_unknown_devices_pct_threshold**. Example: `25`.
 - **crit_unknown_devices_pct_threshold**: float between `0.0` and `100.0`, it indicates the critical warning threshold for high number of devices with "unknown" time slots. A critical warning will be raised if the percentage of these devices with respect to the total number of devices in this date's daily permanence score data is equal or higher than this threshold. Example: `75`.

## Configuration example
```ini
[Logging]
level = DEBUG

[Spark]
session_name = DailyPermanenceScoreQualityMetrics

[DailyPermanenceScoreQualityMetrics]
clear_destination_directory = False

data_period_start = 2023-01-08
data_period_end = 2023-01-09

# A device with a % of "unknown" time intervals equal or greater to this threshold will be considered for the
# warning computation
unknown_intervals_pct_threshold = 50

# Thresholds for percentage of users with high number of "unknown" intervals in a day
non_crit_unknown_devices_pct_threshold = 25
crit_unknown_devices_pct_threshold = 75
```