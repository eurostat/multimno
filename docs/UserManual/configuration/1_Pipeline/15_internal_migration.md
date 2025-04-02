---
title: InternalMigration Configuration
weight: 15
---

# InternalMigration Configuration
To initialise and run the component two configs are used - `general_config.ini` and `internal_migration.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
internal_migration_previous_ue_labels_silver = ${Paths:ue_dir}/internal_migration_previous_ue_labels
internal_migration_new_ue_labels_silver = ${Paths:ue_dir}/internal_migration_new_ue_labels
internal_migration_silver = ${Paths:ue_dir}/internal_migration
# only if used
enriched_grid_data_silver = ${Paths:silver_dir}/grid_enriched
...
```

The expected parameters in `internal_migration.ini` are as follows:
 - **clear_destination_directory**: bool, whether to clear the output directory before running the component. Example: `True`.
 - **clear_quality_metrics_directory**: bool, whether to clear the quality metrics directory before running the component. Example: `False`.
 - **migration_threshold**: float between 0.0 and 1.0, it sets the threshold for considering that a device is to be considered for internal migration based on the metric comparing the set of home tiles in the two long-term time periods being compared. Example: `0.5`.
 - **uniform_tile_weights**: bool, whether to use uniform tile weights for the home-labelled tiles. If `True`, the weights of all home tiles of a device are equal. If `False`, the weights of the home tiles will be proportional to the provided weights, and it is mandatory to provide a path under `internal_migration_enriched_silver_grid` on the `general_config.in` file.
 - **zoning_dataset_id**: string, ID of the zones dataset that will be used to map grid tiles to zones. Example: `nuts`.
 - **hierarchical_levels**: comma-separated list of positive integers, they are the hierarchical levels of the zones dataset used that should be processed by this component. If the zones dataset was not hierarchical, this should just take the value `1`. Example: `1,2,3`.
 - **start_month_previous**: string, in `YYYY-MM` format, it indicates the first month of the first long-term period used to compute the internal migration. Example: `2023-01`.
 - **end_month_previous**: string, in `YYYY-MM` format, it indicates the last month of the first long-term period used to compute the internal migration. Example: `2023-06`.
 - **season_previous**: string, value of the season of the first long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.
 - **start_month_new**: string, in `YYYY-MM` format, it indicates the first month of the second long-term period used to compute the internal migration. Example: `2023-07`.
 - **end_month_new**: string, in `YYYY-MM` format, it indicates the last month of the second long-term period used to compute the internal migration. Example: `2023-12`.
 - **season_new**: string, value of the season of the second long-term period used to compute the internal migration. Allowed values are: `all`, `spring`, `summer`, `autumn`, `winter`. Example: `all`.
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
session_name = InternalMigration

[InternalMigration]
clear_destination_directory = True
clear_quality_metrics_directory = False
migration_threshold = 0.5
uniform_tile_weights = True

zoning_dataset_id = nuts  # ID of the zoning dataset
hierarchical_levels = 1  # Hierarchical level(s) of the zoning dataset. Comma-separated list

start_month_previous = 2023-01  # Start month (inclusive) of the first long-term period
end_month_previous = 2023-06  # End month (inclusive) of the first long-term period
season_previous = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. First long term period

start_month_new = 2023-07  # Start month (inclusive) of the second long-term period
end_month_new = 2023-12  # End month (inclusive) of the second long-term period
season_new = all  # Allowed values: `all`, `spring`, `summer`, `autumn`, `winter`. Second long term period

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
