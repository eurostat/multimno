---
title: CellFootprintQualityMetrics Configuration
weight: 4
---

# CellFootprintQualityMetrics Configuration
To initialise and run the component two configs are used - `general_config.ini` and `cell_footprint_quality_metrics.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
network_data_silver = ${Paths:silver_dir}/mno_network
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
event_data_silver = ${Paths:silver_dir}/mno_event1
...
```

The expected parameters in `cell_footprint_quality_metrics.ini` are as follows:
 - **clear_destination_directory**: bool, whether to clear the output directory before running the component. Example: `True`.
 - **data_period_start**: string, with `YYYY-MM-DD` format, indicating the first date of the date interval for which the cell footprint quality metrics will be computed. All days between **data_period_start** and **data_period_end**, both inclusive, will be processed individually. Example: `2023-01-01`.
 - **data_period_end**: string, with `YYYY-MM-DD` format, indicating the last date of the date interval for which the cell footprint quality metrics will be computed. All days between **data_period_start** and **data_period_end**, both inclusive, will be processed individually. Example: `2023-01-03`.
 - **non_crit_cell_pct_threshold**: float between `0.0` and `100.0`, it indicates the non-critical warning threshold for the percentage of all total cells with no footprint assigned. A non-critical warning will be raised if the percentage of these cells with respect to the total number of cells in this date's network data is strictly higher than this threshold, and strictly lower than **crit_cell_pct_threshold**. Example: `0`.
 - **crit_cell_pct_threshold**: float between `0.0` and `100.0`, it indicates the critical warning threshold for the percentage of all total cells with no footprint assigned. A critical warning will be raised if the percentage of thece cells with respect to the total number of cells in this date's network data is equal or higher than this threshold. Example: `1`.
 - **non_crit_event_pct_threshold**: float between `0.0` and `100.0`, it indicates the non-critical warning threshold for the percentage of all total events assigned to a cell with no footprint. A non-critical warning will be raised if the percentage of these events with respect to the total number of events of this date is strictly higher than this threshold, and strictly lower than **crit_event_pct_threshold**. Example: `0`.
 - **crit_event_pct_threshold**: float between `0.0` and `100.0`, it indicates the non-critical warning threshold for the percentage of all total events assigned to a cell with no footprint. A critical warning will be raised if the percentage of these events with respect to the total number of events of this date is equal or higher than this threshold. Example: `1`.


## Configuration example
```ini
[Spark]
session_name = CellFootprintQualityMetrics

[CellFootprintQualityMetrics]
clear_destination_directory = False

data_period_start = 2023-01-07
data_period_end = 2023-01-11

# Threshold for percentage of total cells with no footprint
non_crit_cell_pct_threshold = 0
crit_cell_pct_threshold = 1

# Threshold for percentage of total events "missed" due to cells with no footprint
non_crit_event_pct_threshold = 0
crit_event_pct_threshold = 1

```

