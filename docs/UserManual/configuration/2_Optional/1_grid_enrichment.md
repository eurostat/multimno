---
title: GridEnrichment Configuration
weight: 1
---

# GridEnrichment Configuration
To initialise and run the component two configs are used - `general_config.ini` and `grid_enrichment.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). 


```ini
[Paths.Bronze]
transportation_data_bronze = ${Paths:bronze_dir}/spatial/transportation
landuse_data_bronze = ${Paths:bronze_dir}/spatial/landuse
[Paths.Silver]
grid_data_silver = ${Paths:silver_dir}/grid
enriched_grid_data_silver = ${Paths:silver_dir}/grid_enriched
```

In grid_enrichment.ini parameters are as follows: 

- **clear_destination_directory** - boolean, if True, the component will clear all the data in output paths.

- **quadkey_batch_size** - integer, the number of quadkeys to process in a single batch. The higher the number, the more memory is required.

- **do_landuse_enrichment** - boolean, if True, the component will enrich the grid with landuse prior probabilities and Path Loss Exponent environment coefficient.

- **transportation_category_buffer_m** - dictionary, buffer distance for each transportation category in meters. Used to convert transportation lines to polygons.

- **do_elevation_enrichment** - boolean, if True, the component will enrich the grid with elevation data. Not implemented yet.


## Configuration example

```ini
[Spark]
session_name = InspireGridGeneration

[InspireGridGeneration]
[Spark]
session_name = GridEnrichment

[GridEnrichment]
clear_destination_directory = True
quadkey_batch_size = 2

do_landuse_enrichment = True
transportation_category_buffer_m = {
    "primary": 30,
    "secondary": 15,
    "tertiary": 5,
    "pedestrian": 5,
    "railroad": 15,
    "unknown": 2
    }

do_elevation_enrichment = False
```
