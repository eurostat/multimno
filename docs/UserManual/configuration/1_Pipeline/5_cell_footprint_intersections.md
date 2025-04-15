---
title: CellFootprintIntersections Configuration
weight: 5
---

# CellFootprintIntersections Configuration
To initialise and run the component two configs are used - `general_config.ini` and `cell_footprint_intersections.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
# input data object
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint

# output data objects
cell_to_group_data_silver = ${Paths:silver_dir}/cell_to_group
group_to_tile_data_silver = ${Paths:silver_dir}/group_to_tile
...
```
The expected parameters in `cell_footprint_intersections.ini` are as follows:

 - **clear_destination_directory**: bool, whether to clear the output directory before running the component. Example: `True`.
 - **data_period_start**: string, in `YYYY-MM-DD` format, it indicates the first date of the date interval for which to compute the cell footprint intersection components. Example: `2023-01-01`.
 - **data_period_end**: string, in `YYYY-MM-DD` format, it indicates the last date of the date interval for which to compute the cell footprint intersection components. Example: `2023-01-03`.

## Configuration example
```ini
[Spark]
session_name = CellFootprintIntersections

[CellFootprintIntersections]
clear_destination_directory = True
data_period_start = 2023-01-01
data_period_end = 2023-01-03
```

