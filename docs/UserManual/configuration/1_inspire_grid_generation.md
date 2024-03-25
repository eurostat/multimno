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

- **grid_mask** - string, the mask to be used for grid generation. It can be either 'extent' or 'polygon'. If 'extent' is chosen, the extent parameter should be provided. If 'polygon' is chosen, the polygon parameter should be provided. Only extent is currently implemented.

- **extent** - dictionary, the extent of the grid to be generated. It should contain the following keys: 'min_lat', 'max_lat', 'min_lon', 'max_lon'.

- **do_landcover_enrichment** - boolean, if True, the component will enrich the grid with landcover data. Default value is False. Currently not implemented.

- **do_elevation_enrichment** - boolean, if True, the component will enrich the grid with elevation data. Default value is False. Currently not implemented.

- **grid_partition_size** - integer, the size of the partition to be used for grid generation as a size of a side of grid subsquare. Default value is 500 grid tiles.

## Configuration example

```ini
[InspireGridGeneration]
grid_mask = 'extent' # 'extent' or 'polygon'
extent = {
    'min_lat': -4.0,
    'max_lat': -3.0,
    'min_lon': 39.0,
    'max_lon': 41.0
    }

do_landcover_enrichment = False
do_elevation_enrichment = False
grid_partition_size = 500
```
