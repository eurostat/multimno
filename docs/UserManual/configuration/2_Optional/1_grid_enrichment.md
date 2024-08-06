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

- **do_landcover_enrichment** - boolean, if True, the component will enrich the grid with landuse prior probabilities and Path Loss Exponent environment coefficient.

- **transportation_category_buffer_m** - dictionary, buffer distance for each transportation category in meters. Used to convert transportation lines to polygons.

- **prior_weights** - dictionary, weights for each landuse category. Used to calculate prior probabilities.

- **ple_coefficient_weights** - dictionary, weights for each landuse category. Used to calculate Path Loss Exponent coefficient.

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
do_landcover_enrichment = True
prior_calculation_repartition_size = 12
transportation_category_buffer_m = {
    "primary": 30,
    "secondary": 15,
    "tertiary": 5,
    "pedestrian": 5,
    "railroad": 15,
    "unknown": 2
    }
prior_weights = {
    "residential_builtup": 1.0,
    "other_builtup": 1.0,
    "roads": 0.5,
    "open_area": 0.0,
    "forest": 0.1,
    "water": 0.0
    }
ple_coefficient_weights = {
    "residential_builtup": 1.0,
    "other_builtup": 1.0,
    "roads": 0.0,
    "open_area": 0.0,
    "forest": 1.0,
    "water": 0.0
    }
do_elevation_enrichment = False
```
