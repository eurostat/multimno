---
title: UsualEnvironmentLabeling Configuration
weight: 15
---

# UsualEnvironmentLabeling Configuration
To initialise and run the component two configs are used - `general_config.ini` and `usual_environment_labeling.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
longterm_permanence_score_data_siilver = ${Paths:silver_dir}/longterm_permanence_score
usual_environment_labels_data_silver = ${Paths:silver_dir}/usual_environment_labels
...
```

The expected parameters in `usual_environment_labeling.ini` are as follows:

 - **start_month**: string, in `YYYY-MM` format, it indicates the first month to be included in the Usual Environment Labeling process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. Example: `2023-01`.

 - **end_month**: string, in `YYYY-MM` format, it indicates the last month to be included in the Usual Environment Labeling process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. Example: `2023-06`.

 - **season**: string, it indicates the season to be included in the Usual Environment Labeling process. Long-term permanence metrics data shall be available for the corresponding start_month, end_month and season. The months that correspond to each season are defined in the configuration of the Long-term permanence score method. Example: `all`. Options:
    - `"all"`: every month between start_month and end_month, both inclusive, are considered in the aggregation.
    - `"winter"`: every month included in the **winter_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"spring"`: every month included in the **spring_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"summer"`: every month included in the **summer_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
    - `"autumn"`: every month included in the **autumn_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation.

 - **total_ps_threshold**: int, permanence score. It represents a long-term permanence score (ps) threshold to apply to discard rarely observed devices. Rarely observed devices are defined as those devices for which the total observed device's ps is lower than this threshold. Example: `1500`.

 - **total_ndays_threshold**: int, number of days. It represents a frequency (number of days) threshold to apply to discard discontinuously observed devices. Discontinuously observed devices are defined as those devices for which the total observed device's frequency is lower than this threshold. Example `50`: 

 - **ue_gap_ps_threshold**: float, percentage. It represents a long-term permanence score (ps) threshold to apply in the preselection of tiles that may be tagged as usual environment (ue). This threshold is relative (a percentage) to the total ps device observation in the corresponding day type and time interval. The tiles associated to each device are sorted by ps in descending order, and then those that are below a ps gap that is bigger than `device_observation_ps * ue_gap_ps_threshold / 100` are filtered out. Example: `20` (%).

 - **gap_ps_threshold**: int, permanence score. It represents a long-term permanence score (ps) threshold to apply in the preselection of tiles that may be tagged as home or work locations. This threshold is an absolute ps value, and its recommended value is 1. The tiles associated to each device are sorted by ps in descending order, and then those that are below a ps gap that is bigger than `gap_ps_threshold` are filtered out. Example: `1`.

 - **ue_ps_threshold**: float, percentage. It represents a long-term permanence score (ps) threshold for the selection of tiles that may be tagged as usual environment (ue). This threshold is relative (a percentage) to the total ps device observation in the corresponding day type and time interval. The tiles associated to one device that have passed the ue gap cut and which have a ps higher than `device_observation_ps * ue_ps_threshold / 100` are tagged as ue. Example: `70` (%).

 - **home_ps_threshold**: float, percentage. It represents a long-term permanence score (ps) threshold for the selection of tiles that may be tagged as home location. This threshold is relative (a percentage) to the total ps device observation in the corresponding day type and time interval. The tiles associated to one device that have passed the home gap cut and which have a ps higher than `device_observation_ps * home_ps_threshold / 100` are tagged as home. Example: `80` (%).

 - **work_ps_threshold**: float, percentage. It represents a long-term permanence score (ps) threshold for the selection of tiles that may be tagged as work location. This threshold is relative (a percentage) to the total ps device observation in the corresponding day type and time interval. The tiles associated to one device that have passed the work gap cut and which have a ps higher than `device_observation_ps * work_ps_threshold / 100` are tagged as work. Example: `80` (%).

 - **ue_ndays_threshold**: float, percentage. It represents a frequency (number of days) threshold for the selection of tiles that may be tagged as usual environment (ue). This threshold is relative (a percentage) to the total observed frequency for the device in the corresponding day type and time interval. The tiles associated to one device that have passed the ue gap cut and which have a frequency higher than device_observation_frequency * ue_ndays_threshold / 100 are tagged as ue. Example: `70` (%).

 - **home_ndays_threshold**: float, percentage. It represents a frequency (number of days) threshold for the selection of tiles that may be tagged as home location. This threshold is relative (a percentage) to the total observed frequency for the device in the corresponding day type and time interval. The tiles associated to one device that have passed the home gap cut and which have a frequency higher than device_observation_frequency * home_ndays_threshold / 100 are tagged as home. Example: `70` (%).

 - **ue_ndays_threshold**: float, percentage. It represents a frequency (number of days) threshold for the selection of tiles that may be tagged as work location. This threshold is relative (a percentage) to the total observed frequency for the device in the corresponding day type and time interval. The tiles associated to one device that have passed the work gap cut and which have a frequency higher than device_observation_frequency * work_ndays_threshold / 100 are tagged as work. Example: `70` (%).

## Configuration example

```ini
[Spark]
session_name = UsualEnvironmentLabeling

[UsualEnvironmentLabeling]
start_month = 2023-01
end_month = 2023-04
season = all

# filtering rarely/discontinuously observed devices
total_ps_threshold = 1500
total_ndays_threshold = 50

ue_gap_ps_threshold = 20
gap_ps_threshold = 1

ue_ps_threshold = 70
home_ps_threshold = 80
work_ps_threshold = 80

ue_ndays_threshold = 70
home_ndays_threshold = 80
work_ndays_threshold = 80
```
