---
title: CellFootprintEstimation Configuration
weight: 5
---

# CellFootprintEstimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `cell_footprint_estimation.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). Example: 

```ini
[Paths.Silver]
signal_strength_data_silver = ${Paths:silver_dir}/signal_strength
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
cell_intersection_groups_data_silver = ${Paths:silver_dir}/cell_intersection_groups
```

In cell_footprint_estimation.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **logistic_function_steepness** - float, the steepness of the logistic function used to estimate the signal dominance (cell footprint) value from signal strength. The default value is 0.2.

- **logistic_function_midpoint** - float, the midpoint of the logistic function used to estimate the signal dominance (cell footprint) value from signal strength. The default value is -92.5.

- **do_difference_from_best_sd_prunning** - boolean, if True, the cells per grid tile with signal dominance values that are lower than the threshold percentage from the best signal dominance will be pruned. If False, no pruning of this method will be performed.

- **difference_from_best_sd_treshold** - float, the threshold percentage from the best signal dominance value. The default value is 90.

- **do_max_cells_per_tile_prunning** - boolean, if True, the maximum number of cells per grid tile will be kept, other cells will be pruned. If False, no pruning of this method will be performed.

- **max_cells_per_grid_tile** - integer, the maximum number of cells per grid tile. The default value is 10.

- **do_sd_treshold_prunning** - boolean, if True, the cells with signal dominance values that are lower than the threshold will be pruned. If False, no pruning of this method will be performed.

- **signal_dominance_treshold** - float, the threshold value for signal dominance. The default value is 0.01.

- **do_cell_intersection_groups_calculation** - boolean, if True, the cell intersection groups will be calculated and corresponding data object created. If False, no cell intersection groups will be calculated.

## Configuration example

```ini
[CellFootprintEstimation]
data_period_start = 2023-01-01
data_period_end = 2023-01-05
logistic_function_steepness = 0.2
logistic_function_midpoint = -92.5

do_difference_from_best_sd_prunning = True
difference_from_best_sd_treshold = 90 # percentage

do_max_cells_per_tile_prunning = False
max_cells_per_grid_tile = 10

do_sd_treshold_prunning = True
signal_dominance_treshold = 0.01

do_cell_intersection_groups_calculation = True
```