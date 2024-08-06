---
title: InspireGridGeneration Configuration
weight: 1
---

# InspireGridGeneration Configuration
To initialise and run the component two configs are used - `general_config.ini` and `grid_generation.ini`. In `general_config.ini` to execute the component specify all paths to its four corresponding data objects (input + output). Example: 


```ini
[Paths.Silver]
grid_data_silver = ${Paths:silver_dir}/grid
```

In grid_generation.ini parameters are as follows: 

- **clear_destination_directory** - boolean, if True, the component will clear all the data in output paths.

- **grid_mask** - string, the mask to be used for grid generation. It can be either 'extent' or 'polygon'. If 'extent' is chosen, the extent parameter should be provided. If 'polygon' is chosen, reference country iso2 code should be provided.

- **extent** - list, the extent of the grid to be generated if 'extent' is chosen spatial mask type. The format is [min_lon, min_lat, max_lon, max_lat].

- **reference_country** - string, iso2 country code to use as a spatial mask for grid generation.

- **country_buffer** - integer, buffer distance to extend country polygon for grid generation.

- **grid_generation_partition_size** - integer, the size of the partition to be used for grid generation as a size of a side of grid subsquare. Default value is 500 grid tiles, so generation will be done with 500 by 500 grid subsquares.

- **grid_processing_partition_quadkey_level** - integer, the level of quadkey for resulted grid partitioning. Default value is level 8.  


## Configuration example

```ini
[Spark]
session_name = InspireGridGeneration

[InspireGridGeneration]
clear_destination_directory = True
grid_mask = polygon # extent or polygon
extent = [-4.5699,39.9101,-2.8544,40.9416] # [min_lon, min_lat, max_lon, max_lat]
reference_country = ES # ISO A2 code
country_buffer = 10000 # meters
grid_generation_partition_size = 500
grid_processing_partition_quadkey_level = 8
```
