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

## General section
The specific component configuration file, `spatial_aggregation.ini`, has three different sections. The general section `[SpatialAggregation]` is used to indicate on what UC outputs the spatial aggregation process should be executed. Its expected parameters are:
 - **present_population_execution**: boolean, it indicates whether the spatial aggregation process should be executed on the gridded present population output. Example: `True`.
 - **usual_environment_execution**: boolean, it indicates whether the spatial aggregation process should be executed on the gridded usual environment output. Example: `True`.

 ## Present Population section
 If the configuration parameter **present_population_execution** has been set to `True`, the component will read the section related to Present Population, `[SpatialAggregation.PresentPopulation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **zoning_dataset_id**: string, ID of the zones dataset to use to aggregate the data from grid level to zone level.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

 ## Usual Environment section
 If the configuration parameter **usual_environment_execution** has been set to `True`, the component will read the section related to Usual Environment, `[SpatialAggregation.UsualEnvironment]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **zoning_dataset_id**: string, ID of the zones dataset to use to aggregate the data from grid level to zone level.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **labels**: comma-separated list of strings, they indicate what usual environment labels should be processed by this component. Allowed values are `home`, `work`, `ue`. Example: `ue, home, work`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-03`.
 - **season**: string, value of the season of the usual environment dataset that the user desires to prcess through this component. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

```ini
[Spark]
session_name = SpatialAggregation

[SpatialAggregation]

present_population_execution = True
usual_environment_execution = True

[SpatialAggregation.PresentPopulation]
clear_destination_directory = True

zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
start_date = 2023-01-08  # start date (inclusive)
end_date = 2023-01-09  # end date (inclusive)

[SpatialAggregation.UsualEnvironment]
clear_destination_directory = True

zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-01  # End month (inclusive)
season = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`.
```
