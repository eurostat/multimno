---
title: TourismStaysEstimation Configuration
weight: 15
---

# TourismStaysEstimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `tourism_stays_estimation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
tourism_stays_estimation_silver = ${Paths:silver_dir}/tourism_stays_estimation
...
```

The expected parameters in `tourism_stays_estimation.ini` are as follows:
 - **clear_destination_directory**: bool, whether to clear the output directory before running the component. Example: `True`.
 - **local_mcc**: int, the Mobile Country Code of the processing home country. Example: `234`.
 - **zoning_dataset_id**: str, the dataset id of the zoning dataset to map time segments cell footrpints to. Example: `'nuts'`.
 - **min_duration_segment_m**: int, the minimum duration of a segment in minutes to be considered as tourism stay. Example: `180`.
 - **functional_midnight_h**: int, the hour of the day to consider as the functional midnight to mark overnight tourism stays. Example: `4`.
 - **min_duration_segment_night_m**: int, the minimum duration of a segment in minutes to be considered as overnight tourism stay. Example: `300`.
 - **filter_ue_segments**: bool, whether to filter out all segments of users who have inbound usual environment. Example: `False`.

## Configuration example
```ini
[Spark]
session_name = TourismStaysEstimation

[InternalMigration]
data_period_start = 2023-01-07
data_period_end = 2023-01-11

clear_destination_directory = true
local_mcc = 234
zoning_dataset_id = 'nuts'
min_duration_segment_m = 180
functional_midnight_h = 4
min_duration_segment_night_m = 300
filter_ue_segments = false
```
