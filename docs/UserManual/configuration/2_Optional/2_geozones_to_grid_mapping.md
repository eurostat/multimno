---
title: GeozonesGridMapping Configuration
weight: 2
---

# GeozonesGridMapping Configuration
To initialise and run the component two configs are used - `general_config.ini` and `geozones_grid_mapping.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). 


```ini
[Paths.Bronze]
geographic_zones_data_bronze = ${Paths:bronze_dir}/spatial/geographic_zones 
admin_units_data_bronze = ${Paths:bronze_dir}/spatial/admin_units 
[Paths.Silver]
grid_data_silver = ${Paths:silver_dir}/grid
geozones_grid_map_data_silver = ${Paths:silver_dir}/geozones_grid_map
```

In geozones_grid_mapping.ini parameters are as follows: 

- **clear_destination_directory** - boolean, if True, the component will clear all the data in output paths.

- **zoning_type** - string, type of zoning data object to be used for mapping. Possible values are "admin" and "other".

- **dataset_ids** - list, ids of zonning datasets to use for mapping. Grid mapping will be done for each dataset separately.

## Configuration example

```ini
[Spark]
session_name = GeozonesGridMapping

[GeozonesGridMapping]
clear_destination_directory = True
zoning_type = other # admin or other
dataset_ids = ['nuts'] # list of dataset ids to map grid to
```
