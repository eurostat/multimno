---
title: TourismStaysEstimation Configuration
weight: 15
---

# TourismStaysEstimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `tourism_stays_estimation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
time_segments_silver = ${Paths:silver_dir}/time_segments
cell_connection_probabilities_data_silver = ${Paths:silver_dir}/cell_conn_probs
geozones_grid_map_data_silver = ${Paths:silver_dir}/geozones_grid_map
tourism_stays_estimation_silver = ${Paths:silver_dir}/tourism_stays_estimation
...
```

The expected parameters in `tourism_stays_estimation.ini` are as follows:
 - **clear_destination_directory**: bool, whether to clear the output directory before running the component. Example: `True`.
 - **zoning_dataset_ids_list**: list of str, the dataset ids of the zoning datasets to map time segments cell footprints to. Example: `'nuts'`.
   - Special dataset names `INSPIRE_1km` and `INSPIRE_100m` map the stays to the corresponding level of grid ids using grid generation. These datasets do not need the `geozones_grid_map_data_silver` input data to be provided.  
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
zoning_dataset_ids_list = ['nuts', 'INSPIRE_1km']
min_duration_segment_m = 180
functional_midnight_h = 4
min_duration_segment_night_m = 300
filter_ue_segments = false
```
