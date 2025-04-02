---
title: UsualEnvironmentAggregation Configuration
weight: 14
---

# UsualEnvironmentAggregation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `ue_aggregation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
usual_environment_labels_data_silver = ${Paths:silver_dir}/usual_environment_labels
grid_data_silver = ${Paths:silver_dir}/grid
aggregated_usual_environments_silver = ${Paths:silver_dir}/aggregated_usual_environment
# only if used
enriched_grid_data_silver = ${Paths:silver_dir}/grid_enriched
...
```

The expected parameters in `ue_aggregation.ini` are as follows:

 - **start_month**: string, in `YYYY-MM` format, it indicates the first month to be included in the Usual Environment Aggregation process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. Example: `2023-01`.

 - **end_month**: string, in `YYYY-MM` format, it indicates the last month to be included in the Usual Environment Aggregation process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. Example: `2023-06`.

 - **season**: string, it indicates the season to be included in the Usual Environment Aggregation process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. The months that correspond to each season are defined in the configuration of the Long-term permanence score method. Example: `all`. Options:
    - `"all"`: every month between start_month and end_month, both inclusive, are considered in the aggregation.
    - `"winter"`: every month included in the **winter_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"spring"`: every month included in the **spring_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"summer"`: every month included in the **summer_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"autumn"`: every month included in the **autumn_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.

 - **clear_destination_directory**: bool, if to delete all previous outputs before running the component.

 - **uniform_tile_weights**: int, if to use uniform tile weights in the aggregation process. If `True`, the weights of the tiles are equal. If `False`, the weights of the tiles are calculated based on landuse information.

- **landuse_prior_weights** - dictionary, keys are land use types, values are weights for these landuse types. The land use types are: 
    - residential_builtup
    - other_builtup
    - roads
    - other_human_activity
    - open_area
    - forest
    - water

## Configuration example

```ini
[Spark]
session_name = UsualEnvironmentLabeling

[UsualEnvironmentLabeling]
clear_destination_directory = True

start_month = 2023-01
end_month = 2023-03

season = all
uniform_tile_weights = True
landuse_weights = {
    "residential_builtup": 1.0,
    "other_builtup": 1.0,
    "roads": 0.5,
    "other_human_activity": 0.1,
    "open_area": 0.0,
    "forest": 0.0,
    "water": 0.0
    }
```
