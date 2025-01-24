---
title: Estimation Configuration
weight: 18
---

# Estimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `estimation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
# Present Population UC data objects
# One of the two parameters below are needed depending on configuration file value `zoning_dataset_id`
present_population_silver = ${Paths:silver_dir}/present_population
present_population_zone_silver = ${Paths:silver_dir}/present_population_zone

estimated_present_population_zone_silver = ${Paths:silver_dir}/estimated_present_population_zone

# Usual Environment UC data objects
# One of the two parameters below are needed depending on configuration file value `zoning_dataset_id`
aggregated_usual_environments_silver = ${Paths:silver_dir}/aggregated_usual_environment
aggregated_usual_environments_zone_silver = ${Paths:silver_dir}/aggregated_usual_environment_zone

estimated_aggregated_usual_environments_zone_silver = ${Paths:silver_dir}/estimated_aggregated_usual_environment_zone

# Internal Migration UC data object
internal_migration_silver = ${Paths:silver_dir}/internal_migration
estimated_internal_migration_silver = ${Paths:silver_dir}/estimated_internal_migration
...
```
For the execution of the Estimation process for the Present Population UC and the Usual Environment (including Home Location UC), one of two possible input datasets may be needed:
 - If the zoning dataset that is provided via configuration file (defined below) is the reserved dataset name `INSPIRE_100m`, the datasets `present_population_silver` and `aggregated_usual_environment_silver` are required for the present population execution and the usual environment execution, respectively.
 - If any other dataset name is provided, the datasets `present_population_zone_silver` and `aggregated_usual_environments_zone_silver` are required for the present population execution and the usual environment execution, respectively.

As of now, the Internal Migration UC execution does not accept the reserved dataset name `INSPIRE_100m`.

## General section
The specific component configuration file, `estimation.ini`, has three different sections. The general section `[Estimation]` is used to indicate on what UC outputs the estimation process should be executed. Its expected parameters are:
 - **present_population_execution**: boolean, it indicates whether the estimation process should be executed on the zone-aggregated present population output. Example: `True`.
 - **usual_environment_execution**: boolean, it indicates whether the estimation process should be executed on the zone-aggregated usual environment output. Example: `True`.
 - **internal_migration_execution** boolean, it indicates whether the estimation process should be executed on the zone-level internal migration output. Example: `True`.

 ## Present Population section
 If the configuration parameter **present_population_execution** has been set to `True`, the component will read the section related to Present Population, `[Estimation.PresentPopulationEstimation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **deduplication_factor**: positive float, this factor will be used to multiply the population value computed at device level to take into account the possibility that a real person might own more than one device. Example: `0.98`.
 - **mno_to_target_population_factor**: positive, float, this factor will be used to multiply the (now deduplicated) population value computed at device level to represent the total target population. Example: `5.4`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific usual environment output data should be processed by this component. The user may also specify the reserved dataset names `INSPIRE_100m`, in which case the `present_population_silver` dataset will be read instead of `present_population_zone_silver`. See above for the execution behaviour under this name. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

 ## Usual Environment section
 If the configuration parameter **usual_environment_execution** has been set to `True`, the component will read the section related to Usual Environment, `[Estimation.UsualEnvironmentAggregation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **deduplication_factor**: positive float, this factor will be used to multiply the weighted device count value computed at device level to take into account the possibility that a real person might own more than one device. Example: `0.98`.
 - **mno_to_target_population_factor**: positive, float, this factor will be used to multiply the (now deduplicated) weighted device count value computed at device level to represent the total target population. Example: `5.4`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific usual environment output data should be processed by this component. The user may also specify the reserved dataset names `INSPIRE_100m`, in which case the `aggregated_usual_environment_silver` dataset will be read instead of `aggregated_usual_environments_zone_silver`. See above for the execution behaviour under this name. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **labels**: comma-separated list of strings, they indicate what usual environment labels should be processed by this component. Allowed values are `home`, `work`, `ue`. Example: `ue, home, work`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-03`.
 - **season**: string, value of the season of the usual environment dataset that the user desires to prcess through this component. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

 ## Internal Migration section
 If the configuration parameter **internal_migration_execution** has been set to `True`, the component will read the section related to Internal Migration, `[Estimation.InternalMigration]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **deduplication_factor**: positive float, this factor will be used to multiply the internal migration metric at device level to take into account the possibility that a real person might own more than one device. Example: `0.98`.
 - **mno_to_target_population_factor**: positive, float, this factor will be used to multiply the (now deduplicated) internal migration value computed at device level to represent the total target population. Example: `5.4`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific internal migration output data should be processed by this component. The Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_month_previous**: string, in `YYYY-MM` format, it indicates the first month of the first long-term period used to compute the internal migration. Example: `2023-01`.
 - **end_month_previous**: string, in `YYYY-MM` format, it indicates the last month of the first long-term period used to compute the internal migration. Example: `2023-06`.
 - **season_previous**: string, value of the season of the first long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.
 - **start_month_new**: string, in `YYYY-MM` format, it indicates the first month of the second long-term period used to compute the internal migration. Example: `2023-07`.
 - **end_month_new**: string, in `YYYY-MM` format, it indicates the last month of the second long-term period used to compute the internal migration. Example: `2023-12`.
 - **season_new**: string, value of the season of the second long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

## Configuration example
```ini
[Spark]
session_name = Estimation


[Estimation]
present_population_execution = True
usual_environment_execution = True

[Estimation.PresentPopulationEstimation]
clear_destination_directory = True
deduplication_factor = 0.95
mno_to_target_population_factor = 5.4

# Target PresentPopulationEstimation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
start_date = 2023-01-01  # start date (inclusive)
end_date = 2023-01-01  # end date (inclusive)


[Estimation.UsualEnvironmentAggregation]
clear_destination_directory = True
deduplication_factor = 0.95
mno_to_target_population_factor = 5.4

# Target UsualEnvironmentAggregation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-01  # End month (inclusive)
season = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`.


[Estimation.InternalMigration]
clear_destination_directory = True
deduplication_factor = 0.95
mno_to_target_population_factor = 5.4

# Target InternalMigration dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_month_previous = 2023-01  # Start month (inclusive) of the first long-term period
end_month_previous = 2023-06  # End month (inclusive) of the first long-term period
season_previous = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. First long term period

start_month_new = 2023-07  # Start month (inclusive) of the second long-term period
end_month_new = 2023-12  # End month (inclusive) of the second long-term period
season_new = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. Second long term period
```