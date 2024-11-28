---
title: DailyPermanenceScore Configuration
weight: 10
---

# DailyPermanenceScore Configuration
To initialise and run the component two configs are used -  `general_config.ini` and component’s config `daily_permanence_score.ini`. In  `general_config.ini` all paths to the data objects used by DailyPermanenceScore component shall be specified. An example with the specified paths is shown below:


```ini
[Paths.Silver]
...
event_data_silver_flagged = ${Paths:silver_dir}/mno_events_flagged
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
daily_permanence_score_data_silver = ${Paths:silver_dir}/daily_permanence_score
...
```

Below there is a description of the sub component’s config  - `daily_permanence_score.ini`. 

Parameters are as follows:

Under  `[DailyPermanenceScore]` config section: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-08), the date from which start DailyPermanenceScore processing. Make sure events and cell footprint data are available for all the dates between data_period_start and data_period_end, as well as for the previous day to data_period_start and the posterior day to data_period_end. 

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-15), the date till which we will perform DailyPermanenceScore processing. Make sure events and cell footprint data are available for all the dates between data_period_start and data_period_end, as well as for the previous day to data_period_start and the posterior day to data_period_end. 

- **time_slot_number** - integer, can take the values 24, 48, or 96. Number of equal-length time slots in which to divide each date for the daily permanence score calculation. Recommended value: 24, so that the day is divided in 24 1-hour time slots. The values 48 and 96 result in 30-min and 15-min time slots, respectively.

- **max_time_thresh** - integer, in seconds. In case of 2 consecutive events taking place in different cells, if the time difference between the events is lower than this threshold, then the both the first event's assigned end time and the second event's assigned start time will be equal to the average between the first and second event's timestamps. If the time difference between the 2 consecutive events is higher than this threshold, then the assigned end time for the first event will be equal to the first event's timestamp plus half the value of this threshold; and the assigned start time for the second event will be equal to the second event's timestamp minus half the value of this threshold. For the case of 2 consecutive events taking place in different cells, if the time difference between the events is higher than the corresponding threshold (either `max_time_thresh_day` or `max_time_thresh_night`), then the event timestamps are also extended half this value of `max_time_thresh`. Recommended value: 900 seconds (15 minutes).

- **max_time_thresh_day** - integer, in seconds. In case of 2 consecutive events in the same cell, if the time of at least one of the events is included in the "day time", i.e. from 9:00 to 22:59, then `max_time_thresh_day` is the threshold to be applied. If the time difference between the events is lower than this threshold, then the both the first event's assigned end time and the second event's assigned start time will be equal to the average between the first and second event's timestamps. Otherwise, the assigned end time for the first event will be equal to the first event's timestamp plus half the value of `max_time_thresh`; and the assigned start time for the second event will be equal to the second event's timestamp minus half the value of `max_time_thresh`. Recommended value: 7200 seconds (2 hours).

- **max_time_thresh_night** - integer, in seconds. In case of 2 consecutive events in the same cell, if the time of both events is included in the "night time", i.e. from 23:00 to 8:59, then `max_time_thresh_night` is the threshold to be applied. If the time difference between the events is lower than this threshold, then the both the first event's assigned end time and the second event's assigned start time will be equal to the average between the first and second event's timestamps. Otherwise, the assigned end time for the first event will be equal to the first event's timestamp plus half the value of `max_time_thresh`; and the assigned start time for the second event will be equal to the second event's timestamp minus half the value of `max_time_thresh`. Recommended value: 28800 seconds (8 hours).

- **max_vel_thresh** - float, in metres per second (m/s). In order to evaluate if an event corresponds to a "move", the speed between the previous event and the next event is calculated. If the speed exceeds `max_vel_thresh`, then the event is tagged as a move and will be discarded for daily permanence score calculation. Recommended value: 13.889 m/s (50 km/h).

- **event_error_flags_to_include** - set of integers, e.g. "{0}". It indicates the values of the "error_flag" column of the input event data that will be kept. Rows with "error_flag" values not included in this set will be discarded and will not be considered for any step of the daily permanence score component. Recommended value: {0}.


## Configuration example

```ini
[Logging]
level = DEBUG

[Spark]
session_name = DailyPermanenceScore

[DailyPermanenceScore]
data_period_start = 2023-01-02
data_period_end = 2023-01-02

time_slot_number = 24

max_time_thresh = 900  # 15 min
max_time_thresh_day = 7_200  # 2 h
max_time_thresh_night = 28_800  # 8 h

max_speed_thresh = 13.88888889  # 50 km/h

event_error_flags_to_include = {0}
```