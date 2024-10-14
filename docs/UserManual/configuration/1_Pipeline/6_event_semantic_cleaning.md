---
title: SemanticCleaning Configuration
weight: 6
---

# SemanticCleaning Configuration
To initialise and run the component two configs are used - `general_config.ini` and `event_semantic_cleaning.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
network_data_silver = ${Paths:silver_dir}/mno_network
event_data_silver_flagged = ${Paths:silver_dir}/mno_events_flagged
event_device_semantic_quality_metrics = ${Paths:silver_quality_metrics_dir}/semantic_quality_metrics
...
```

The expected parameters in `event_semantic_cleaning.ini` are as follows:
- **data_period_start**: string, format should be the one specified `data_period_format` (e.g., `2023-01-01` for `%Y-%m-%d`), the first date for which data will be processed by the component. All dates between this one and the specified in `data_period_end` will be processed (both inclusive).
- **data_period_end**: string, format should be "yyyy-MM-dd" (e.g., `2023-01-09` for `%Y-%m-%d`), the last date for which data will be processed by the component. All dates between the specified in `data_period_start` and this one will be processed (both inclusive).
- **data_period_format**: string, it indicates the format expected in `data_period_start` and `data_period_end`. For example, use `%Y-%m-%d` for the usual "2023-01-09" format separated by `-`.
- **semantic_min_distance_m**: float, minimum distance (in metres) between two consecutive events above which they will be considered for flagging as suspicious or incorrect location. Example: `10000`.
- **semantic_min_speed_m_s**: float, minimum mean speed (in metres per second) between two consecutive events above whihc they will be considered for flagging as suspicious or incorrect location. Example: `55`.
- **do_different_location_deduplication**: boolean, True/False. Determines whether to flag duplicates with different location information (cases where a single user has one or more rows with identical timestamp values, but non-identical values in any other columns).

## Configuration example

```ini
[Spark]
session_name = SemanticCleaning

[SemanticCleaning]
data_period_start = 2023-01-01
data_period_end = 2023-01-09
date_format = %Y-%m-%d

semantic_min_distance_m = 10000
semantic_min_speed_m_s = 55

do_different_location_deduplication = True
```