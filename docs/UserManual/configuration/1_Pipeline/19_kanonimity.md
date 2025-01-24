---
title: KAnonimity Configuration
weight: 19
---

# KAnonimity Configuration
To initialise and run the component two configs are used - `general_config.ini` and `kanonimity.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
# Present Population UC input data object
estimated_present_population_zone_silver = ${Paths:silver_dir}/estimated_present_population_zone

# Usual Environment UC input data objects
estimated_aggregated_usual_environments_zone_silver = ${Paths:silver_dir}/estimated_aggregated_usual_environment_zone

# Internal Migration UC input data object
estimated_internal_migration_silver = ${Paths:silver_dir}/estimated_internal_migration
...

[Paths.Gold]
# Present Population UC output data object
kanonimity_present_population_zone_gold = ${Paths:gold_dir}/kanonimity_present_population_zone

# Usual Environment UC output data object
kanonimity_aggregated_usual_environments_zone_gold = ${Paths:gold_dir}/kanonimity_aggregated_usual_environment_zone

# Internal Migration UC output data object
kanonimity_internal_migration_gold = ${Paths:silver_dir}/kanonimity_internal_migration
...
```

## General section
The specific component configuration file, `kanonimity.ini`, has three different sections. The general section `[KAnonimity]` is used to indicate on what UC outputs the k-anonimity process should be executed. Its expected parameters are:
 - **present_population_execution**: boolean, it indicates whether the k-anonimity process should be executed on the zone-aggregated present population output. Example: `True`.
 - **usual_environment_execution**: boolean, it indicates whether the k-anonimity process should be executed on the zone-aggregated usual environment output. Example: `True`.
 - **internal_migration_execution** boolean, it indicates whether the k-anonimity process should be executed on the zone-level internal migration output. Example: `True`.

 ## Present Population section
 If the configuration parameter **present_population_execution** has been set to `True`, the component will read the section related to Present Population, `[KAnonimity.PresentPopulationEstimation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **k**: integer, it indicates the threshold value to select what records should have k-anonimity applied to them, that is, all records that have a population value strictly lower than **k**. Example: `5`.
 - **anonimity_type**: string, it indicates what k-anonimity metholodogy to employ. If it is set to `delete`, it will delete all records that have a population value strictly lower than **k**. If it is set to `obfuscate`, it will replace all population values strictly lower than **k** by the negative value `-1`. Example: `delete`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific present population output data should be processed by this component. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

 ## Usual Environment section
 If the configuration parameter **usual_environment_execution** has been set to `True`, the component will read the section related to Usual Environment, `[KAnonimity.UsualEnvironmentAggregation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **k**: integer, it indicates the threshold value to select what records should have k-anonimity applied to them, that is, all records that have a weighted device count value strictly lower than **k**. Example: `5`.
 - **anonimity_type**: string, it indicates what k-anonimity metholodogy to employ. If it is set to `delete`, it will delete all records that have a weighted device count value strictly lower than **k**. If it is set to `obfuscate`, it will replace all weighted device count values strictly lower than **k** by the negative value `-1`. Example: `delete`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific usual environment output data should be processed by this component. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **labels**: comma-separated list of strings, they indicate what usual environment labels should be processed by this component. Allowed values are `home`, `work`, `ue`. Example: `ue, home, work`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-03`.
 - **season**: string, value of the season of the usual environment dataset that the user desires to prcess through this component. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

 ## Internal Migration section
 If the configuration parameter **internal_migration_execution** has been set to `True`, the component will read the section related to Internal Migration, `[KAnonimity.InternalMigration]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **k**: integer, it indicates the threshold value to select what records should have k-anonimity applied to them, that is, all records that have an internal migration value strictly lower than **k**. Example: `5`.
 - **anonimity_type**: string, it indicates what k-anonimity metholodogy to employ. If it is set to `delete`, it will delete all records that have an internal migration value strictly lower than **k**. If it is set to `obfuscate`, it will replace all internal migration values strictly lower than **k** by the negative value `-1`. Example: `delete`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was used to map grid tiles to zones. This identifies what specific internal migration output data should be processed by this component. Example: `nuts`.
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
session_name = KAnonimity

[KAnonimity]
present_population_execution = True
usual_environment_execution = True
internal_migration_execution = True


[KAnonimity.PresentPopulationEstimation]
clear_destination_directory = True
anonimity_type = obfuscate  # `obfuscate`, `delete`
k = 5

# Target PresentPopulationEstimation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
start_date = 2023-01-01  # start date (inclusive)
end_date = 2023-01-01  # end date (inclusive)

[KAnonimity.UsualEnvironmentAggregation]
clear_destination_directory = True
anonimity_type = obfuscate  # `obfuscate`, `delete`
k = 5

# Target UsualEnvironmentAggregation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-03  # End month (inclusive)
season = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`.

[KAnonimity.InternalMigration]
clear_destination_directory = True
anonimity_type = obfuscate  # `obfuscate`, `delete`
k = 5

# Target InternalMigration dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_month_previous = 2023-01  # Start month (inclusive) of the first long-term period
end_month_previous = 2023-06  # End month (inclusive) of the first long-term period
season_previous = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. First long term period

start_month_new = 2023-07  # Start month (inclusive) of the second long-term period
end_month_new = 2023-12  # End month (inclusive) of the second long-term period
season_new = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. Second long term period
```