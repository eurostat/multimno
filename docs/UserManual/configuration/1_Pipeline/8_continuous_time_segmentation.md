---
title: ContinuousTimeSegmentation Configuration
weight: 8
---

# ContinuousTimeSegmentation Configuration
To initialise and run the component two configs are used - `general_config.ini` and `time_segments.ini`. In `general_config.ini` to execute the component specify all paths to its four corresponding data objects (input + output). Example: 


```ini
[Paths.Silver]
event_data_silver_flagged = ${Paths:silver_dir}/mno_events_flagged
event_cache = ${Paths:silver_dir}/mno_events_cache
cell_intersection_groups_data_silver = ${Paths:silver_dir}/cell_intersection_groups
time_segments_silver = ${Paths:silver_dir}/time_segments
```

In time_segments.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **clear_time_segments_directory** - boolean, if True, the component will delete any existing time segments before calculation. If False, existing time segments will be used as input data where relevant. 

- **event_error_flags_to_include** - list of integers, the list of error flags that should be included in the time segments processing. Default value is [0], so only events with no errors are included.

- **min_time_stay_s** - integer, the minimum dwell time in seconds for a time segments to be considered as a "stay". Default value is 15 minutes.

- **max_time_missing_stay_s** - integer, maximum time difference between events to be considered a “stay”. If larger, the time segment will be marked “unknown”. Default value is 12 hours to support devices being offline at home or work addresses.

- **max_time_missing_move_s** - integer, maximum time difference between events to be considered a “move”. If larger, the time segment will be marked “unknown”. Default value is 2 hours.

- **max_time_missing_abroad_s** - integer, maximum time difference between events to be considered a “abroad”. If larger, the time segment will be marked “unknown”. Default value is 72 hours.

- **pad_time_s** - integer, half the size of an isolated time segment: between two “unknowns” time segments. It expands the isolated event in time, by “padding” from the “unknown” time segments on both sides. Default value is 5 minutes.

- **domains_to_include** - list[string], List of event domains that will be processed. Allowed values "inbound", "domestic", "outbound".


## Configuration example

```ini
[ContinuousTimeSegmentation]
data_period_start = 2023-01-01
data_period_end = 2023-01-05

is_first_run = true
event_error_flags_to_include = [0]

min_time_stay_s = 900
max_time_missing_stay_s = 43200
max_time_missing_move_s = 7200
max_time_missing_abroad_s = 259200
pad_time_s = 300

domains_to_include = ["inbound", "domestic", "outbound"]
```
