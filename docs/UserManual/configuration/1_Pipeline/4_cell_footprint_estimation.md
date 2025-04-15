---
title: CellFootprintEstimation Configuration
weight: 4
---

# CellFootprintEstimation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `cell_footprint_estimation.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). Example: 

```ini
[Paths.Silver]
network_data_silver = ${Paths:silver_dir}/mno_network
grid_data_silver = ${Paths:silver_dir}/grid
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
```

In cell_footprint_estimation.ini parameters are as follows: 

- **clear_destination_directory** - boolean, if True, the component will clear all the data in output paths.

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the first date of the date interval for which the cell footprint estimation will be executed.

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the last date of the date interval for which the cell footprint estimation will be executed.

- **logistic_function_steepness** - float, the steepness of the logistic function used to estimate the signal dominance (cell footprint) value from signal strength. Example: 0.2.

- **logistic_function_midpoint** - float, the midpoint of the logistic function used to estimate the signal dominance (cell footprint) value from signal strength. Example: -92.5.

- **use_elevation** - boolean, if True, the elevation data will be used for signal strength modeling. If False, the elevation will be set to 0.

- **do_azimuth_angle_adjustments** - boolean, if True, signal strengths values will be adjusted based on azimuth and antenna horizontal beam widths for directional cells. If False, no adjustments will be made.

- **do_elevation_angle_adjustments** - boolean, if True, signal strengths values will be adjusted based on tilt and antenna vertical beam widths for directional cells. If False, no adjustments will be made.

- **cartesian_crs** - integer, the coordinate reference system (CRS) to use for the cartesian coordinates. Example: 3035.

- **do_percentage_of_best_sd_pruning** - boolean, if True, the cells per grid tile with signal dominance values that are strictly lower than the threshold percentage of best signal dominance will be pruned. If False, no pruning of this method will be performed.

- **percentage_of_best_sd_threshold** - float between 0 and 100, the threshold percentage from the best signal dominance value. For example, if this threshold is set to 90 (percent) and the highest signal dominance in a tile is 1.0, then all signal dominances
with values strictly lower than 0.9 will be discarded (since $0.9 = 90\% \text{ of } 1.0$). Example: 90.
- **do_max_cells_per_tile_pruning** - boolean, if True, there will be a maximum number of cells per grid tile that will be kept, other cells will be pruned. If False, no pruning of this method will be performed.

- **max_cells_per_grid_tile** - integer, the maximum number of cells per grid tile. Example: 10.

- **do_dynamic_coverage_range_calculation** - boolean, if True, the component will calculate the dynamic coverage range for each cell based on signal dominance threshold value. If False, default range will be used. Might be useful to reduce computational load, but might take longer to calculate.

- **do_sd_threshold_pruning** - boolean, if True, the cells with signal dominance values that equal or lower than the threshold will be pruned. If False, no pruning of this method will be performed.

- **signal_dominance_threshold** - float, the threshold value for signal dominance. Example: 0.01.

- **coverage_range_line_buffer** - integer, the buffer distance of cell range line in meters for dynammic range calculation. The value should be set based on reference grid resolution. Example: 50.

- **repartition_number** - integer, the number of partitions to use for the repartitioning of the data during dynamic range calculations. Example: 10.

- **default_cell_physical_properties** - dictionary, the default physical properties of the cell types. These properties will be assigned to cells of corresponding type if the properties are not found in the network topology data. If cell types are not peresent in network topology data, the default type properties will be assigned to all cells.

## Configuration example

```ini
[CellFootprintEstimation]
clear_destination_directory = True
data_period_start = 2023-01-01
data_period_end = 2023-01-01
logistic_function_steepness = 0.2
logistic_function_midpoint = -92.5

use_elevation = False
do_azimuth_angle_adjustments = True
do_elevation_angle_adjustments = True
cartesian_crs = 3035

do_percentage_of_best_sd_pruning = False
percentage_of_best_sd_threshold = 90 # percentage

do_max_cells_per_tile_pruning = False
max_cells_per_grid_tile = 100

do_dynamic_coverage_range_calculation = False
signal_dominance_threshold = 0.01
coverage_range_line_buffer = 50
repartition_number = 10

do_sd_threshold_pruning = True

default_cell_physical_properties = {
    'macrocell': {
        'power': 10,
        'range': 10000,
        'path_loss_exponent': 3.75,
        'antenna_height': 30,
        'elevation_angle': 5,
        'vertical_beam_width': 9,
        'horizontal_beam_width': 65,
        'azimuth_signal_strength_back_loss': -30,
        'elevation_signal_strength_back_loss': -30,
    },
    'microcell': {
        'power': 5,
        'range': 1000,
        'path_loss_exponent': 6.0,
        'antenna_height': 8,
        'elevation_angle': 5,
        'vertical_beam_width': 9,
        'horizontal_beam_width': 65,
        'azimuth_signal_strength_back_loss': -30,
        'elevation_signal_strength_back_loss': -30,
    },
    'default': {
        'power': 5,
        'range': 5000,
        'path_loss_exponent': 3.75,
        'antenna_height': 8,
        'elevation_angle': 5,
        'vertical_beam_width': 9,
        'horizontal_beam_width': 65,
        'azimuth_signal_strength_back_loss': -30,
        'elevation_signal_strength_back_loss': -30,
        }
    }
```