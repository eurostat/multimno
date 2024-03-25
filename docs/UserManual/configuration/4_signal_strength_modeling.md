---
title: SignalStrengthModeling Configuration
weight: 4
---

# SignalStrengthModeling Configuration
To initialise and run the component two configs are used - `general_config.ini` and `signal_strength_modeling.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). Example:

```ini
[Paths.Silver]
network_data_silver = ${Paths:silver_dir}/mno_network
grid_data_silver = ${Paths:silver_dir}/grid
signal_strength_data_silver = ${Paths:silver_dir}/signal_strength
```

In signal_strength_modeling.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **use_elevation** - boolean, if True, the elevation data will be used for signal strength modeling. If False, the elevation will be set to 0

- **do_azimuth_angle_adjustments** - boolean, if True, signal strengths values will be adjusted based on azimuth and antenna horizontal beam widths for directional cells. If False, no adjustments will be made.

- **do_elevation_angle_adjustments** - boolean, if True, signal strengths values will be adjusted based on tilt and antenna vertical beam widths for directional cells. If False, no adjustments will be made.

- **cartesian_crs** - integer, the coordinate reference system (CRS) to use for the cartesian coordinates. The default value is 3035.

- **default_cell_physical_properties** - dictionary, the default physical properties of the cell types. These properties will be assigned to cells of corresponding type if the properties are not found in the network topology data. If cell types are not peresent in network topology data, the default type properties will be assigned to all cells.

## Configuration example
```ini
[SignalStrengthModeling]
data_period_start = 2023-01-01
data_period_end = 2023-01-15
use_elevation = False
do_azimuth_angle_adjustments = True
do_elevation_angle_adjustments = True
cartesian_crs = 3035

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
