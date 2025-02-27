---
title: MultiMNOAggregation Configuration
weight: 20
---

# MultiMNOAggregation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `multimno_aggregation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. In this aggregation component that brings together the data computed individually in several MNOs, the number of input data paths depends on the number of different MNOs that the user worked with. This number will be specified below in the configuration section related to the relevant use case. Example (data from two MNOs):

```ini
...
[Paths.Gold]
# Present Population UC data objects
single_mno_1_present_population_zone_gold = ${Paths:gold_dir}/present_population_MNO_1
single_mno_2_present_population_zone_gold = ${Paths:gold_dir}/present_population_MNO_2
multimno_aggregated_present_population_zone_gold = ${Paths:gold_dir}/multimno_present_population

# Usual Environment / Home location UC data objects
single_mno_1_usual_environment_zone_gold = ${Paths:gold_dir}/usual_environment_MNO_1
single_mno_2_usual_environment_zone_gold = ${Paths:gold_dir}/usual_environment_MNO_2
multimno_aggregated_usual_environment_zone_gold = ${Paths:gold_dir}/multimno_usual_environment

# Internal migration UC data objects
single_mno_1_internal_migration_gold = ${Paths:gold_dir}/internal_migration_MNO_1
single_mno_2_internal_migration_gold = ${Paths:gold_dir}/internal_migration_MNO_2
multimno_internal_migration_gold = ${Paths:gold_dir}/multimno_internal_migration

# Inbound tourism UC data objects
single_mno_1_inbound_tourism_zone_aggregations_gold = ${Paths:gold_dir}/inbound_tourism_zone_aggregations_MNO_1
single_mno_2_inbound_tourism_zone_aggregations_gold = ${Paths:gold_dir}/inbound_tourism_zone_aggregations_MNO_2
multimno_inbound_tourism_zone_aggregations_gold = ${Paths:gold_dir}/multimno_inbound_tourism_zone_aggregations

# Outbound tourism UC data objects
single_mno_1_outbound_tourism_aggregations_gold = ${Paths:gold_dir}/outbound_tourism_aggregations_MNO_1
single_mno_2_outbound_tourism_aggregations_gold = ${Paths:gold_dir}/outbound_tourism_aggregations_MNO_2
multimno_outbound_tourism_aggregations_gold = ${Paths:gold_dir}/multimno_outbound_tourism_aggregations
...
```

## General section
Under the general section, the user may specify for which use case's outputs the process will be executed and other parameters that are common to all use cases. The following parameters are expected:
 - **use_case**: string, it specifies the use case whose outputs will be processed by the component. Currently there are five accepted values:
   - `PresentPopulationEstimation` for the Present Population use case
   - `UsualEnvironmentAggregation` for the Usual Environment use case
   - `InternalMigration` for the Internal Migration use case
   - `TourismStatisticsCalculation` for the Inbound Tourism use case
   - `TourismOutboundStatisticsCalculation` for the Outbound Tourism use case
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **number_of_single_mnos**: positive integer equal or greater than `2`, it indicates the number of MNOs whose data will be aggregated by this component. Example: `2`. This number also dictates:
   - The number of input data paths that need to be specified in the general configuration. The name format of these configuration parameters is `single_mno_i_present_population_zone_gold` (for the Present Population use case; name changes depending on the use case, see example above), where `i` must be an integer between `1` and **number_of_single_mnos**. All input data path parameters are mandatory. See the configuration example above for the case where there are two MNOs.
   - The number of MNO factors. The name format of these configuration parameters is `single_mno_i_factor`, where `i` must be an integer between `1` and **number_of_single_mnos**. All factor parameters are mandatory and are described below.
 - **single_mno_`i`_factor**: float between 0 and 1, this parameter (for all values `i` between `1` and **number_of_single_mnos**) represents the weight that the indicator produced by this specific MNO will have in the final aggregation. Example: `0.6`. See the configuration example below for the case where **number_of_single_mnos** is equal to `2`.

 ## Present Population section
In this section, the user may configure exactly what part of the output of the present population use case will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset that the present population data refers to.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. The list of levels is ignored and set to be `1` if a different value is encountered when **zoning_dataset_id** is one of the reserved dataset names. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

 ## Usual Environment section
In this section, the user may configure exactly what part of the output of the usual environment use case (also encompassing the home location use case) will be processed by the component.
 - **zoning_dataset_id**: string, ID of the zones dataset that the usual environment data refers to.
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
session_name = MultiMNOAggregation


[MultiMNOAggregation]
use_case = PresentPopulationEstimation
clear_destination_directory = True
number_of_single_mnos = 2
single_mno_1_factor = 0.6
single_mno_2_factor = 0.4


[MultiMNOAggregation.PresentPopulationEstimation]
# Target PresentPopulationEstimation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
start_date = 2023-01-08  # start date (inclusive)
end_date = 2023-01-08  # end date (inclusive)


[MultiMNOAggregation.UsualEnvironmentAggregation]
# Target UsualEnvironmentAggregation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-01  # End month (inclusive)
season = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`.


[MultiMNOAggregation.InternalMigration]
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_month_previous = 2023-01  # Start month (inclusive) of the first long-term period
end_month_previous = 2023-06  # End month (inclusive) of the first long-term period
season_previous = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. First long term period

start_month_new = 2023-07  # Start month (inclusive) of the second long-term period
end_month_new = 2023-12  # End month (inclusive) of the second long-term period
season_new = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. Second long term period


[MultiMNOAggregation.TourismStatisticsCalculation]
zoning_dataset_id = nuts
hierarchical_levels = 1,2,3

start_month = 2023-01
end_month = 2023-01


[MultiMNOAggregation.TourismOutboundStatisticsCalculation]
start_month = 2023-01
end_month = 2023-02
```