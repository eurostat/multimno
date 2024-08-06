---
title: PresentPopulationEstimation Configuration
weight: 11
---


# DailyPermanenceScore Configuration
To initialise and run the component two configs are used -  `general_config.ini` and component’s config `present_population_estimation.ini`. In  `general_config.ini` all paths to the data objects used by DailyPermanenceScore component shall be specified. An example with the specified paths is shown below:


```ini
[Paths.Silver]
...
grid_data_silver = ${Paths:silver_dir}/grid
cell_connection_probabilities_data_silver = ${Paths:silver_dir}/cell_connection_probabilities_data_silver
event_data_silver_flagged = ${Paths:silver_dir}/mno_events_flagged
present_population_silver = ${Paths:silver_dir}/present_population
present_population_zone_silver = ${Paths:silver_dir}/present_population_zone
zone_to_grid_map_silver = ${Paths:silver_dir}/zone_to_grid_map
...
```

Below there is a description of the sub component’s config  - `present_population_estimation.ini`. 

Parameters are as follows:

Under  `[PresentPopulationEstimation]` config section: 

- **data_period_start** - string, format should be “yyyy-MM-dd HH:mm:ss“ (e.g. 2023-01-08 12:30:00). Determines when the first time point is generated.
- **data_period_end** - string, format should be “yyyy-MM-dd HH:mm:ss“ (e.g. 2023-01-08 12:30:00). No time points can be generated after this time. A time point can be generated at this exact time.
- **time_point_gap_s** - integer, in seconds. Determines the interval between two time points. Starting from `data_period_start`, one time point is generated after each `time_point_gap_s` seconds until `data_period_end` is reached.
- **nr_of_user_id_partitions** - integer. Total number of user_id_modulo partitions. This should be equal to the number of partitions that user event data has been split into-  
- **nr_of_user_id_partitions_per_slice** - integer. Number of user_id_modulo partitions to process at one time. Should be adjusted to optimize between processing speed and memory usage limitations.
- **tolerance_period_s** - integer, in seconds. Determines the size of the temporal window of each time point. Only events within this distance from the time point are included in the results calculation of that point. 
- **max_iterations** - integer. Maximum number of iteration allowed for the Bayesian process for each time point.
- **min_difference_threshold** - float. Minumum difference between Bayesian process prior and posterior population estimates needed to continue iterating the process.
- **output_aggregation_level** - string, value is either "grid" or "zone". Determines whether the final results are aggregated per grid tile or per zoning area. 
- **zoning_dataset_id** - string. Name of the zoning data to use, has to match the `dataset_id` column in the grid to zone mapping dataset. Only needed when output_aggregation_level is "zone".
- **zoning_hierarchical_level** - integer. Level of hierarchial zoning to aggregate results to. The corresponding level from the `hierarchical_id` column in the grid to zone mapping dataset is used. Only needed when output_aggregation_level is "zone".
 

## Configuration example: grid-level aggregation

```ini
[Logging]
level = DEBUG

[Spark]
session_name = PresentPopulationEstimationGrid

[PresentPopulationEstimation]
data_period_start = 2023-01-01 00:00:00 # Starting bound when to create time points. The first time point is created at this timestamp. 
data_period_end = 2023-01-02 00:00:00 # Ending bound when to create time points. No time points are generated later than this timestamp. A time point can happen to be generated on this timestamp, but this is not always the case.
time_point_gap_s = 43200 # space between consecutive time points
tolerance_period_s = 3600 # Maximum allowed time difference for an event to be included in a time point
nr_of_user_id_partitions = 128 # Total number of user_id_modulo partitions. TODO should be a global conf value
nr_of_user_id_partitions_per_slice = 32 # Number of user_id_modulo partitions to process at one time
max_iterations = 20 # Number of iterations allowed for the Bayesian process
min_difference_threshold = 10000 # Minimum total difference between Bayesian process prior and posterior needed to continue processing 
output_aggregation_level = grid # Supported values: "grid", "zone". Determines which level the results are aggregated to.
```

## Configuration example: zone-level aggregation
```ini
[Logging]
level = DEBUG

[Spark]
session_name = PresentPopulationEstimationZone

[PresentPopulationEstimation]
data_period_start = 2023-01-01 00:00:00 # Starting bound when to create time points. The first time point is created at this timestamp. 
data_period_end = 2023-01-02 00:00:00 # Ending bound when to create time points. No time points are generated later than this timestamp. A time point can happen to be generated on this timestamp, but this is not always the case.
time_point_gap_s = 43200 # space between consecutive time points
tolerance_period_s = 3600 # Maximum allowed time difference for an event to be included in a time point
nr_of_user_id_partitions = 128 # Total number of user_id_modulo partitions. TODO should be a global conf value
nr_of_user_id_partitions_per_slice = 32 # Number of user_id_modulo partitions to process at one time
max_iterations = 20 # Number of iterations allowed for the Bayesian process
min_difference_threshold = 10000 # Minimum total difference between Bayesian process prior and posterior needed to continue processing 
output_aggregation_level = zone # Supported values: "grid", "zone". Determines which level the results are aggregated to.
zoning_dataset_id = zoning_01 # Name of zoning dataset. Only needed when output_aggregation_level is "zone".
zoning_hierarchical_level = 3 # Level of hierarchial zoning to aggregate results to. Only needed when output_aggregation_level is "zone".
```
