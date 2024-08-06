---
title: SyntheticDiaries Configuration
weight: 1
---

# SyntheticDiaries Configuration
To initialise and run the component two configs are used - `general_config.ini` and `synthetic_diaries.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Bronze]
...
diaries_data_bronze = ${Paths:bronze_dir}/diaries
...
```

The expected parameters in `synthetic_diaries.ini` are as follows:
 - **number_of_users**: integer, number of devices for which to generate synthetic diaries in each target date. Example: `100`.
 - **date_format**: string, format for date parsing from **initial_date** field (1989 C standard). Example: `%Y-%m-%d`.
 - **initial_date**: string, initial date for which to generate synthetic diaries, matching the date format specified in **date_format**. Example: `2023-01-01`.
 - **number_of_dates**: integer, number of dates for which synthetic diaries will be generated. Synthetic diaries for **initial_date** and for the following `number_of_dates - 1` dates will be generated. If diaries are to be generated for just one date, set this parameter equal to 1. Example: `9`.

 - **latitude_min**: float, degrees. Lower limit of the bounding box within which all locations will be generated. Example: `40.352`.
 - **latitude_max**: float, degrees. Upper limit of the bounding box within which all locations will be generated. Must be higher than **latitude_min**. Example: `40.486`.
 - **longitude_min**: float, degrees. Left limit of the bounding box within which all locations will be generated. Example: `-3.751`.
 - **longitude_max**: float, degrees. Right limit of the bounding box within which all locations will be generated. Must be higher than **longitude_min**. Example: `-3.579`.

 - **home_work_distance_min**: float, meters. Work location of a user will be generated at a distance higher than this minimum from the user's home location. Example: `2000` (m).
 - **home_work_distance_max**: float, meters. Work location of a user will be generated at a distance lower than this maximum from the user's home location. Must be higher than **home_work_distance_min**. Example: `10000` (m).
 - **other_distance_min**: float, meters. For activities of type 'other', the location of the activity will be generated at a distance higher than this minimum from the user's previous activity. Example: `1000` (m).
 - **other_distance_max**: float, meters. For activities of type 'other', the location of the activity will be generated at a distance lower than this maximum from the user's previous activity. Must be higher than **other_distance_min**. Example: `3000` (m).

 - **home_duration_min**: float, hours. This is the minimum duration that will be considered for an activity with stay type 'home'. Example: `5` (hours).
 - **home_duration_max**: float, hours. This is the maximum duration that will be considered for an activity with stay type 'home'. Must be higher than **home_duration_min**. Example: `12` (hours).
 - **work_duration_min**: float, hours. This is the minimum duration that will be considered for an activity with stay type 'work'. Example: `4` (hours).
 - **work_duration_max**: float, hours. This is the maximum duration that will be considered for an activity with stay type 'work'. Must be higher than **work_duration_min**. Example: `8` (hours).
 - **other_duration_min**: float, hours. This is the minimum duration that will be considered for an activity with stay type 'other'. Example: `1` (hours).
 - **other_duration_max**: float, hours. This is the maximum duration that will be considered for an activity with stay type 'other'. Must be higher than **other_duration_min**. Example: `3` (hours).

 - **displacement_speed**: float, m/s. Given the location of 2 consecutive generated activities for a user, this speed helps us define the second activity's start time once we know the first activity's end time. The distance between both activities is calculated and then divided by this speed in order to calculate the trip time, which is then added to the first activity's end time in order to obtain the second activity's initial time. Example: `3` (m/s).


## Configuration example

```ini
[Spark]
# parameters in this section are passed to Spark except session_name 
session_name = SyntheticDiarySession

[SyntheticDiaries]
number_of_users = 100
date_format = %Y-%m-%d
initial_date = 2023-01-01
number_of_dates = 9

latitude_min = 40.352
latitude_max = 40.486
longitude_min = -3.751
longitude_max = -3.579

home_work_distance_min = 2_000  # in meters
home_work_distance_max = 10_000  # in meters
other_distance_min = 1_000  # in meters
other_distance_max = 3_000  # in meters
home_duration_min = 5  # in hours
home_duration_max = 12  # in hours
work_duration_min = 4  # in hours
work_duration_max = 8  # in hours
other_duration_min = 1  # in hours
other_duration_max = 3  # in hours
displacement_speed = 3  # 3 m/s = 10 km/h

stay_sequence_superset = home,other,work,other,other,other,home
stay_sequence_probabilities = 1,0.1,0.6,0.4,0.2,0.1,1
```