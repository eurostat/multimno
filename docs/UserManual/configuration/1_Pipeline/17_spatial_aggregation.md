---
title: SpatialAggregation Configuration
weight: 16
---

# SpatialAggregation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `spatial_aggregation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
geozones_grid_map_data_silver = ${Paths:silver_dir}/geozones_grid_map
aggregated_usual_environments_silver = ${Paths:silver_dir}/aggregated_usual_environment
aggregated_usual_environments_zone_silver = ${Paths:silver_dir}/aggregated_usual_environment_zone
present_population_silver = ${Paths:silver_dir}/present_population
present_population_zone_silver = ${Paths:silver_dir}/present_population_zone
...
```

The expected parameters in `spatial_aggregation.ini` are as follows:

- **clear_destination_directory**: bool, if to delete all previous outputs before running the component.
- **zonning_dataset_id**: str, Geozones dataset ID to be used for aggregation.
- **hierarchical_levels**: str, In case of hierarchical zones, the levels to be used for aggregation. If not hierarchical, set to 1.
- **aggregation_type**: str, Data object to be aggregated. Currently supported: present_population, usual_environments

## Configuration example

```ini
[Spark]
session_name = SpatialAggregation

[SpatialAggregation]
clear_destination_directory = True

zonning_dataset_id = nuts
hierarchical_levels = 1,2,3,4 

aggregation_type = usual_environments 
```
