---
title: TourismOutboundStatisticsCalculation Configuration
weight: 21
---

# TourismOutboundStatisticsCalculation Configuration
To initialise and run the component two configs are used: `general_config.ini` and `tourism_outbound_statistics_calculation.ini`. 


## General configuration

In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
...
[Paths.Silver]
...
time_segments_silver = ${Paths:silver_dir}/time_segments
mcc_iso_timezones_data_bronze = ${Paths:bronze_dir}/mcc_iso_timezones
tourism_outbound_trips_silver = ${Paths:silver_dir}/tourism_outbound_trips
tourism_outbound_aggregations_silver = ${Paths:silver_dir}/tourism_outbound_aggregations
...
```
`time_segments_silver`, `mcc_iso_timezones_data_bronze` and `tourism_outbound_trips_silver` are input data paths.

`tourism_outbound_trips_silver` and `tourism_outbound_aggregations_silver` are output data paths.

`tourism_outbound_trips_silver` is both the input and output path and it does not need to contain data for the component to be executed, but its data can be used as input if it does.

## Configuration parameters
The component configuration file `tourism_outbound_statistics_calculation.ini` has three sections:

`Spark` and `Logging` contain generic session name and logging parameters. 

The section `TourismOutboundStatisticsCalculation` controls component logic and contains the following parameters:

- **data_period_start**: YYYY-MM format string. Indicates the first month for which the component will generate results for. Example: `2023-01`.  
- **data_period_end**: YYYY-MM format string. Indicates the last month for which the component will generate results for. Example: `2023-02`.
- **clear_destination_directory**: Boolean. Indicates if existing results should be deleted before execution. If True, existing data in path `tourism_outbound_aggregations_silver` will be deleted before calculations start. Example: `True`.
- **delete_existing_trips**: Boolean. Indicates if existing trips (from previous executions) should be deleted before execution. If True, existing data in path `tourism_outbound_trips_silver` will be deleted before calculations start. If they are not deleted, they may be used as input data during the execution. Example: `False`.
- **max_outbound_trip_gap_h**: Integer. Maximum number of hours allowed between two time segments for them to be possibly marked as part of the same trip. Additionally is used to determine the size of the look-forward window to retrieve next month entries when processing monthly data. Example: `24`.
- **min_duration_segment_m**: Integer. Minimum duration in minutes for a time segment to be used as input data. Example: `72`.
- **functional_midnight_h**: Integer. Hour of day acting as the functional midnight. Example: `4`.
- **min_duration_segment_night_m**: Integer. Minimum duration in minutes for a time segment to be possibly marked as an overnight segment. Example: `200`.
 - **filter_ue_segments**: bool, whether to filter out all segments of users who have inbound usual environment. Example: `False`.

## Configuration example
```ini
[Logging]
level = DEBUG

[Spark]
session_name = TourismOutboundStatisticsCalculation

[TourismOutboundStatisticsCalculation]
data_period_start = 2023-01
data_period_end = 2023-02
clear_destination_directory = true
delete_existing_trips = False
max_outbound_trip_gap_h = 72
min_duration_segment_m = 180
functional_midnight_h = 4
min_duration_segment_night_m = 200
filter_ue_segments = false
```