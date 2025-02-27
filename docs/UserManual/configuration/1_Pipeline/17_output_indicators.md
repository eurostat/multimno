---
title: OutputIndicators Configuration
weight: 17
---

# OutputIndicators Configuration
To initialise and run the component two configs are used - `general_config.ini` and `output_indicators.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. For example:

```ini
[Paths.Bronze]
# Optional dataset for Inbound Tourism use case, contains deduplication and mno-to-target-population factors
# for nationalities that visit the local country
inbound_estimation_factors_bronze = ${Paths:bronze_dir}/inbound_estimation_factors

[Paths.Silver]
# Only needed for Present Population and Usual Environment use cases, when spatial aggregation is to be performed
# for a custom zoning (not INSPIRE grid)
geozones_grid_map_data_silver = ${Paths:silver_dir}/geozones_grid_map

# Present Population
present_population_silver = ${Paths:silver_dir}/present_population

# Usual Environment
aggregated_usual_environments_silver = ${Paths:ue_dir}/aggregated_usual_environment

# Internal Migration
internal_migration_silver = ${Paths:ue_dir}/internal_migration

# Inbound Tourism
tourism_geozone_aggregations_silver = ${Paths:silver_dir}/tourism_geozone_aggregations
tourism_trip_aggregations_silver = ${Paths:silver_dir}/tourism_trip_aggregations

# Outbound Tourism
tourism_outbound_aggregations_silver = ${Paths:silver_dir}/tourism_outbound_aggregations


[Paths.Gold]
# Present Population
present_population_zone_gold = ${Paths:gold_dir}/present_population_zone_gold

# Usual Environment
aggregated_usual_environments_zone_gold = ${Paths:gold_dir}/aggregated_usual_environments_zone_gold

# Internal Migration
internal_migration_gold = ${Paths:gold_dir}/internal_migration

# Inbound Tourism
tourism_geozone_aggregations_gold = ${Paths:gold_dir}/tourism_geozone_aggregations
tourism_trip_aggregations_gold = ${Paths:gold_dir}/tourism_trip_aggregations

# Outbound Tourism
tourism_outbound_aggregations_gold = ${Paths:gold_dir}/tourism_outbound_aggregations
```

The OutputIndicators component can be executed for one of five use cases, processing the output data objects of that use case and preparing them to be exported out of the MNO premises. The execution is configured through the `output_indicators.ini` file, that contains the following sections.

## General section
Under the general section, the user may specify for which use case outputs the process will be executed and other parameters that are common to all use cases. The following parameters are expected:
 - **use_case**: string, it specifies the use case whose outputs will be processed by the component. Currently there are five accepted values:
   - `PresentPopulationEstimation` for the Present Population use case
   - `UsualEnvironmentAggregation` for the Usual Environment use case
   - `InternalMigration` for the Internal Migration use case
   - `TourismStatisticsCalculation` for the Inbound Tourism use case
   - `TourismOutboundStatisticsCalculation` for the Outbound Tourism use case
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.

 - **deduplication_factor_local**: float, this factor will be used to multiply the appropriate values computed at device level to take into account the possibility that a real person might own more than one device. In particular, this factor **will be applied to indicators that are based on devices that are part of the local MNO network**. Example: `0.98`.
 - **deduplication_factor_default_inbound**: float, this factor will be used to multiply the appropriate values computed at device level to take into account the possibility that a real person might own more than one device. In particular, this factor **will be applied to indicators that are based on devices that belong to an MNO of a country other than the local one**, and will only be used as a **default value for countries whose factor is not specified in the InboundEstimationFactors** data object. Example: `0.95`.
 - **mno_to_target_population_factor_local**: float, this factor will be used to multiply the (now deduplicated) population value computed at device level to represent the total target population. In particular, this factor **will be applied to indicators that are based on devices that are part of the local MNO network** .Example: `5.4`.
 - **mno_to_target_population_factor_default_inbound**: float, this factor will be used to multiply the (now deduplicated) population value computed at device level to represent the total target population. In particular, this factor **will be applied to indicators that are based on devices that belong to an MNO of a country other than the local one**, and will only be used as a **default value for countries whose factor is not specified in the InboundEstimationFactors** data object. Example: `4.2`.
 - **k**: integer, it indicates the threshold value to select what records should have k-anonymity applied to them, that is, all records that have a population value strictly lower than **k**. Example: `5`.
 - **anonymity_type**: string, it indicates what k-anonymity metholodogy to employ. If it is set to `delete`, it will delete all records that have a population value strictly lower than **k**. If it is set to `obfuscate`, it will replace all population values strictly lower than **k** by the negative value `-1`. Example: `delete`.

## Present Population section
In this section, the user may configure exactly what part of the output of the present population use case will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset to use to aggregate the data from grid level to zone level. The user may also specify one of the reserved dataset names `INSPIRE_100m` and `INSPIRE_1km`, in which case no `geozones_grid_map_data_silver` needs to be read.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

## Usual Environment section
In this section, the user may configure exactly what part of the output of the usual environment use case (also encompassing the home location use case) will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset to use to aggregate the data from grid level to zone level. The user may also specify one of the reserved dataset names `INSPIRE_100m` and `INSPIRE_1km`, in which case no `geozones_grid_map_data_silver` needs to be read.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **labels**: comma-separated list of strings, they indicate what usual environment labels should be processed by this component. Allowed values are `home`, `work`, `ue`. Example: `ue, home, work`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the period used to compute the specific usual environment dataset that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the period used to compute the specific usual environment dataset that the user desires to process through this component. Example: `2023-03`.
 - **season**: string, value of the season of the usual environment dataset that the user desires to process through this component. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

 ## Internal Migration section
In this section, the user may configure exactly what part of the output of the home location use case will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset that the internal migration data refers to.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_month_previous**: string, in `YYYY-MM` format, it indicates the first month of the first long-term period used to compute the internal migration. Example: `2023-01`.
 - **end_month_previous**: string, in `YYYY-MM` format, it indicates the last month of the first long-term period used to compute the internal migration. Example: `2023-06`.
 - **season_previous**: string, value of the season of the first long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.
 - **start_month_new**: string, in `YYYY-MM` format, it indicates the first month of the second long-term period used to compute the internal migration. Example: `2023-07`.
 - **end_month_new**: string, in `YYYY-MM` format, it indicates the last month of the second long-term period used to compute the internal migration. Example: `2023-12`.
 - **season_new**: string, value of the season of the second long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

 ## Inbound Tourism section
In this section, the user may configure exactly what part of the output of the inbound tourism use case will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset that the inbound tourism data refers to.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the month interval of inbound tourism data that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the month interval of inbound tourism data that the user desires to process through this component. Example: `2023-03`.

 ## Outbound Tourism section
In this section, the user may configure exactly what part of the output of the outbound tourism use case will be processed by the component.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the month interval of outbound tourism data that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the month interval of outbound tourism data that the user desires to process through this component. Example: `2023-03`.

 ## Configuration example

```ini
[Spark]
session_name = OutputIndicators

[OutputIndicators]

use_case = InternalMigration

clear_destination_directory = False
deduplication_factor_local = 0.95
deduplication_factor_default_inbound = 0.95
mno_to_target_population_factor_local = 5.4
mno_to_target_population_factor_default_inbound = 4.2
anonymity_type = obfuscate  # `obfuscate`, `delete`
k = 5


[OutputIndicators.InternalMigration]
# Target InternalMigration dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_month_previous = 2023-01  # Start month (inclusive) of the first long-term period
end_month_previous = 2023-06  # End month (inclusive) of the first long-term period
season_previous = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. First long term period

start_month_new = 2023-07  # Start month (inclusive) of the second long-term period
end_month_new = 2023-12  # End month (inclusive) of the second long-term period
season_new = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. Second long term period


[OutputIndicators.PresentPopulationEstimation]
# Target InternalMigration dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_date = 2023-01-03  # start date (inclusive)
end_date = 2023-01-03  # end date (inclusive)


[OutputIndicators.UsualEnvironmentAggregation]
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-06  # End month (inclusive)
season = all


[OutputIndicators.TourismStatisticsCalculation]
zoning_dataset_id = nuts
hierarchical_levels = 1,2,3

start_month = 2023-01
end_month = 2023-02


[OutputIndicators.TourismOutboundStatisticsCalculation]
start_month = 2023-01
end_month = 2023-02
```