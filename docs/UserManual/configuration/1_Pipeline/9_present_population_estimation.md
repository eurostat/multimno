---
title: PresentPopulationEstimation Configuration
weight: 9
---


# PresentPopulationEstimation Configuration
To initialise and run the component two configs are used -  `general_config.ini` and component’s config `present_population_estimation.ini`. In  `general_config.ini` all paths to the data objects used by PresentPopulationEstimation component shall be specified. An example with the specified paths is shown below:


```ini
[Paths.Silver]
...
grid_data_silver = ${Paths:silver_dir}/grid
cell_connection_probabilities_data_silver = ${Paths:silver_dir}/cell_connection_probabilities_data_silver
event_data_silver_flagged = ${Paths:silver_dir}/mno_events_flagged
present_population_silver = ${Paths:silver_dir}/present_population
...
```

Below there is a description of the sub component’s config  - `present_population_estimation.ini`. 

Parameters are as follows:

Under  `[PresentPopulationEstimation]` config section: 

- **data_period_start** - string, format should be “yyyy-MM-dd HH:mm:ss“ (e.g. 2023-01-08 12:30:00). Determines when the first time point is generated.
- **data_period_end** - string, format should be “yyyy-MM-dd HH:mm:ss“ (e.g. 2023-01-08 12:30:00). No time points can be generated after this time. A time point can be generated at this exact time.
- **time_point_gap_s** - integer, in seconds. Determines the interval between two time points. Starting from `data_period_start`, one time point is generated after each `time_point_gap_s` seconds until `data_period_end` is reached.
- **tolerance_period_s** - integer, in seconds. Determines the size of the temporal window of each time point. Only events within this distance from the time point are included in the results calculation of that point. 
- **max_iterations** - integer. Maximum number of iteration allowed for the Bayesian process for each time point.
- **min_difference_threshold** - float. Minumum difference between Bayesian process prior and posterior population estimates needed to continue iterating the process.

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
```

