---
title: MultiMNOAggregation Configuration
weight: 20
---

# MultiMNOAggregation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `multimno_aggregation.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. In this aggregation component that brings together the data computed individually in several MNOs, the number of input data paths depends on the number of different MNOs that the user worked with. This number will be specified below in the configuration section related to the relevant use case. Example (two MNOs for both Present Population and Usual Environment):

```ini
...
[Paths.Gold]
# Present Population UC data objects
single_mno_1_present_population_zone_gold = ${Paths:gold_dir}/kanonimity_present_population_zone_1
single_mno_2_present_population_zone_gold = ${Paths:gold_dir}/kanonimity_present_population_zone_2
multimno_aggregated_present_population_zone_gold = ${Paths:gold_dir}/multimno_aggregated_present_population_zone

# Usual Environment UC data objects
single_mno_1_usual_environment_zone_gold = ${Paths:gold_dir}/kanonimity_aggregated_usual_environment_zone_1
single_mno_2_usual_environment_zone_gold = ${Paths:gold_dir}/kanonimity_aggregated_usual_environment_zone_2
multimno_aggregated_usual_environment_zone_gold = ${Paths:gold_dir}/multimno_aggregated_usual_environment_zone

# Internal Migration UC data objects
single_mno_1_internal_migration_gold = ${Paths:gold_dir}/kanonimity_internal_migration_1
single_mno_2_internal_migration_gold = ${Paths:gold_dir}/kanonimity_internal_migration_2
multimno_internal_migration_gold = ${Paths:gold_dir}/multimno_internal_migration
...
```

## General section
The specific component configuration file, `multimno_aggregation.ini`, has three different sections. The general section `[MultiMNOAggregation]` is used to indicate on what UC outputs the multi-MNO aggregation process should be executed. Its expected parameters are:
 - **present_population_execution**: boolean, it indicates whether the multi-MNO aggregation process should be executed on the zone-aggregated present population output. Example: `True`.
 - **usual_environment_execution**: boolean, it indicates whether the multi-MNO aggregation process should be executed on the zone-aggregated usual environment output. Example: `True`.
 - **internal_migration_execution** boolean, it indicates whether the multi-MNO aggregation process should be executed on the zone-level internal migration output. Example: `True`.

 ## Present Population section
 If the configuration parameter **present_population_execution** has been set to `True`, the component will read the section related to Present Population, `[MultiMNOAggregation.PresentPopulationEstimation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **number_of_single_mnos**: positive integer equal or greater than `2`, it indicates the number of MNOs whose present population data will be aggregated by this component. Example: `2`. This number also dictates:
   - The number of input data paths that need to be specified in the general configuration. The name format of these configuration parameters is `single_mno_i_present_population_zone_gold`, where `i` must be an integer between `1` and **number_of_single_mnos**. All input data path parameters are mandatory. See the configuration example above for the case where there are two MNOs.
   - The number of MNO factors. The name format of these configuration parameters is `single_mno_i_factor`, where `i` must be an integer between `1` and **number_of_single_mnos**. All factor parameters are mandatory and are described below.
 - **single_mno_`i`_factor**: float between 0 and 1, this parameter (for all values `i` between `1` and **number_of_single_mnos**) represents the weight that the indicator produced by this specific MNO will have in the final aggregation. It is required that all MNO factors add up to `1`. Example: `0.6`. See the configuration example below for the case where **number_of_single_mnos** is equal to `2`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific present population output data should be processed by this component. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_date**: string, in `YYYY-MM-DD` format, indicates the starting date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.
 - **end_date**: string, in `YYYY-MM-DD` format, indicates the ending date (inclusive) for which the present population data should be processed by this component. Example: `2023-01-01`.

 ## Usual Environment section
 If the configuration parameter **usual_environment_execution** has been set to `True`, the component will read the section related to Usual Environment, `[MultiMNOAggregation.UsualEnvironmentAggregation]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **number_of_single_mnos**: positive integer equal or greater than `2`, it indicates the number of MNOs whose usual environment data will be aggregated by this component. Example: `2`. This number also dictates:
   - The number of input data paths that need to be specified in the general configuration. The name format of these configuration parameters is `single_mno_i_usual_environment_zone_gold`, where `i` must be an integer between `1` and **number_of_single_mnos**. All input data path parameters are mandatory. See the configuration example above for the case where there are two MNOs.
   - The number of MNO factors. The name format of these configuration parameters is `single_mno_i_factor`, where `i` must be an integer between `1` and **number_of_single_mnos**. All factor parameters are mandatory and are described below.
 - **single_mno_`i`_factor**: float between 0 and 1, this parameter (for all values `i` between `1` and **number_of_single_mnos**) represents the weight that the indicator produced by this specific MNO will have in the final aggregation. It is required that all MNO factors add up to `1`. Example: `0.6`. See the configuration example below for the case where **number_of_single_mnos** is equal to `2`.
 - **zoning_dataset_id**: string, ID of the zones dataset that was previously used to aggregate the data from grid level to zone level. This identifies what specific usual environment output data should be processed by this component. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **labels**: comma-separated list of strings, they indicate what usual environment labels should be processed by this component. Allowed values are `home`, `work`, `ue`. Example: `ue, home, work`.
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month of the period used to compute a specific usual environment dataset that the user desires to process through this component. Example: `2023-03`.
 - **season**: string, value of the season of the usual environment dataset that the user desires to prcess through this component. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.

 ## Internal Migration section
 If the configuration parameter **internal_migration_execution** has been set to `True`, the component will read the section related to Internal Migration, `[MultiMNOAggregation.InternalMigration]`. The expected parameters in this section are as follows:
 - **clear_destination_directory**: boolean, whether to delete all previous results in the output directory before running the component.
 - **number_of_single_mnos**: positive integer equal or greater than `2`, it indicates the number of MNOs whose internal migration data will be aggregated by this component. Example: `2`. This number also dictates:
   - The number of input data paths that need to be specified in the general configuration. The name format of these configuration parameters is `single_mno_i_internal_migration_gold`, where `i` must be an integer between `1` and **number_of_single_mnos**. All input data path parameters are mandatory. See the configuration example above for the case where there are two MNOs.
   - The number of MNO factors. The name format of these configuration parameters is `single_mno_i_factor`, where `i` must be an integer between `1` and **number_of_single_mnos**. All factor parameters are mandatory and are described below.
 - **single_mno_`i`_factor**: float between 0 and 1, this parameter (for all values `i` between `1` and **number_of_single_mnos**) represents the weight that the indicator produced by this specific MNO will have in the final aggregation. It is required that all MNO factors add up to `1`. Example: `0.6`. See the configuration example below for the case where **number_of_single_mnos** is equal to `2`.
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
session_name = MultiMNOAggregation


[MultiMNOAggregation]
present_population_execution = True
usual_environment_execution = True
internal_migration_execution = True

[MultiMNOAggregation.PresentPopulationEstimation]
clear_destination_directory = True
number_of_single_mnos = 2
single_mno_1_factor = 0.6
single_mno_2_factor = 0.4

# Target PresentPopulationEstimation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
start_date = 2023-01-01  # start date (inclusive)
end_date = 2023-01-01  # end date (inclusive)

[MultiMNOAggregation.UsualEnvironmentAggregation]
clear_destination_directory = True
number_of_single_mnos = 2
single_mno_1_factor = 0.6
single_mno_2_factor = 0.4

# Target UsualEnvironmentAggregation dataset
zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1,2,3,4  # Hierarchical level(s) of the zoning dataset. Comma-separated list
labels = ue, home, work  # Allowed values: `ue`, `home`, `work`. Comma-separated list
start_month = 2023-01  # Start month (inclusive)
end_month = 2023-03  # End month (inclusive)
season = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`.

[MultiMNOAggregation.InternalMigration]
clear_destination_directory = True
number_of_single_mnos = 2
single_mno_1_factor = 0.6
single_mno_2_factor = 0.4

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