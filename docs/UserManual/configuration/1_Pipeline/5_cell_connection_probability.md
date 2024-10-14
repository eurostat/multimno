---
title: CellConnectionProbabilityEstimation Configuration
weight: 5
---

# CellConnectionProbabilityEstimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `cell_connection_probability_estimation`.ini. In `general_config.ini` to execute the component specify all paths to its four corresponding data objects (input + output). Example: 

```ini
[Paths.Bronze]
event_data_bronze = ${Paths:bronze_dir}/mno_events

[Paths.Silver]
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
grid_data_silver = ${Paths:silver_dir}/grid
cell_connection_probabilities_data_silver = ${Paths:silver_dir}/cell_conn_probs
```

In cell_connection_probability_estimation.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **use_land_use_prior** - boolean, if True, the land use prior will be used for cell connection posterior probability estimation. If False, the land use prior will not be used, only connection probability based on cell footprint will be estimated.

## Configuration example

```ini
[CellConnectionProbabilityEstimation]
data_period_start = 2023-01-01
data_period_end = 2023-01-15
use_land_use_prior = False
```
