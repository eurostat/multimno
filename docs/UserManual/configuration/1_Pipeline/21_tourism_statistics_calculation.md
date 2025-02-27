---
title: TourismStatisticsCalculation Configuration
weight: 21
---

# TourismStatisticsCalculation Configuration
To initialise and run the component two configs are used: `general_config.ini` and `tourism_statistics_calculation.ini`. 

## General configuration

In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
...
[Paths.Silver]
...
tourism_stays_silver = ${Paths:silver_dir}/tourism_stays
mcc_iso_timezones_data_bronze = ${Paths:bronze_dir}/mcc_iso_timezones
tourism_trips_silver = ${Paths:silver_dir}/tourism_trips
tourism_geozone_aggregations_silver = ${Paths:silver_dir}/tourism_geozone_aggregations
tourism_trip_aggregations_silver = ${Paths:silver_dir}/tourism_trip_aggregations
...
```
`tourism_stays_silver`, `mcc_iso_timezones_data_bronze` and `tourism_trips_silver` are input data paths.

`tourism_geozone_aggregations_silver`, `tourism_trip_aggregations_silver` and `tourism_trips_silver` are output data paths.

`tourism_trips_silver` is both the input and output path and it does not need to contain data for the component to be executed, but its data can be used as input if it does.

## Configuration parameters

The configuration file `tourism_statistics_calculation.ini` has three sections: 

`Spark` and `Logging` contain generic session name and logging parameters. 

The section `TourismStatisticsCalculation` controls component logic and contains the following parameters:

- **data_period_start**: YYYY-MM format string. Indicates the first month for which the component will generate results for. Example: `2023-01`.  
- **data_period_end**: YYYY-MM format string. Indicates the last month for which the component will generate results for. Example: `2023-02`.
- **clear_destination_directory**: Boolean. Indicates if existing results should be deleted before execution. If True, existing data in paths `tourism_geozone_aggregations_silver` and `tourism_trip_aggregations_silver` will be deleted before calculations start. Example: `True`.
- **delete_existing_trips**: Boolean. Indicates if existing trips (from previous executions) should be deleted before execution. If True, existing data in path `tourism_trips_silver` will be deleted before calculations start. If they are not deleted, they may be used as input data during the execution. Example: `False`.
- **zoning_dataset_ids_and_levels_list**: List of zoning dataset name and hierarchical level pairs. Each entry pair in the list should specify the name of a zoning dataset and the list of the hierarchical levels to calculate results for. For single-level datasets (such as `INSPIRE_1KM`), the hierarchical level should be `[1]`. Example:`[('test_dataset',[1,2,3])]`.
- **max_trip_gap_h**: Integer. Maximum number of hours allowed between two stays for them to be possibly marked as part of the same trip. Additionally is used to determine the size of the look-forward window to retrieve next month entries when processing monthly data. Example: `24`.
- **max_visit_gap_h**: Integer. Maxmimum number of hours allowed between two stays for them to be possibly marked as part of the same visit. Example: `24`.


## Configuration example
```ini
[Logging]
level = DEBUG

[Spark]
session_name = TourismStatisticsCalculation

[TourismStatisticsCalculation]
data_period_start = 2023-01
data_period_end = 2023-02
clear_destination_directory = true
delete_existing_trips = False
zoning_dataset_ids_and_levels_list = [('test_dataset',[1,2,3])]
max_trip_gap_h = 24
max_visit_gap_h = 24

```