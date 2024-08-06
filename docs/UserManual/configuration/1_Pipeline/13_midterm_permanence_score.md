---
title: MidtermPermanenceScore Configuration
weight: 13
---

# MidtermPermanenceScore Configuration
To initialise and run the component two configs are used - `general_config.ini` and `midterm_permanence_score.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
...
[Paths.Bronze]
holiday_calendar_data_bronze = ${Paths:bronze_dir}/holiday_calendar

...

[Paths.Silver]
cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
daily_permanence_score_data_silver = ${Paths:silver_dir}/daily_permanence_score
midterm_permanence_score_data_siilver = ${Paths:silver_dir}/midterm_permanence_score
...
```

The expected parameters in `midterm_permanence_score.ini` are as follows:
 - **start_month**: string, with `YYYY-MM` format, indicating the first month for which mid-term permanence score and metrics are to be computed. All months between **start_month** and **end_month**, both inclusive, will be processed individually. Example: `2023-01`.
 - **end_month**: string, with `YYYY-MM` format, indicating the last month for which mid-term permanence score and metrics are to be computed. All months between **start_month** and **end_month**, both inclusive, will be processed individually. Must be a month equal or later than **start_month**. Example: `2023-08`.
 - **before_regularity_days**: positive integer, it represents the number of days previous to a month that will be loaded and used in the calculation of the mid-term regularity metrics by searching for the latest date in which a given user performed a stay in a given grid tile. Example: `7`.
 - **after_regularity_days**: positive integer, it represents the number of days following a month that will be loaded and used in the calculation of the mid-term regularity metrics by searching for the earliest date in which a given user performed a stay in a given grid tile. Example: `7`.
 - **day_start_hour**: integer between 0 and 23 (both inclusive) that marks the starting time of a given day in the mid-term analysis to be performed. Example: `4`. A value of `4` means that the time slots of the Daily Permanence Score contained between the 04:00 of day $D$ (inclusive) and the 04:00 of day $D+1$ (not inclusive) are considered to belong to day $D$.
 - **country_of_study**: two-letter, upper-case string, marking the ISO Alpha 2 code of the country being studied, used to load the holiday dates of that country that are used to define the *workdays* and *holidays* day types. Example: `ES` (Spain).
 - **weekend_start**: integer between 1 and 7, marks the first day of the week (inclusive) that belongs to the *weekends* day type. Monday is 1, Tuesday is 2, ..., and Sunday is 7. Example: `6`.
 - **weekend_end**: integer between 1 and 7, marks the last day of the week (inclusive) that belongs to the *weekends* day type. Monday is 1, Tuesday is 2, ..., and Sunday is 7. Example: `7`.
 - **night_time_start**: string, `HH:MM` format, marking the starting hour and minutes (inclusive) of the *night_time* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `18:00`.
 - **night_time_end**: string, `HH:MM` format, marking the ending hour and minutes (exclusive) of the *night_time* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `08:00`.
 - **working_hours_start**: string, `HH:MM` format, marking the starting hour and minutes (inclusive) of the *working_hours* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `07:30`.
 - **working_hours_end**: string, `HH:MM` format, marking the ending hour and minutes (exclusive) of the *working_hours* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `17:30`.
 - **evening_time_start**: string, `HH:MM` format, marking the starting hour and minutes (inclusive) of the *evening_time* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `18:00`.
 - **evening_time_end**: string, `HH:MM` format, marking the ending hour and minutes (exclusive) of the *evening_time* time interval. Only allowed values for the minutes `MM` are `00`, `15`, `30`, or `45`. The time interval limits must be compatible with the time slot limits and durations of the Daily Permanence Score data used. See the [Time interval additional information](#time-interval-additional-information) for additional restrictions of this parameter. Example: `21:00`.
 - **period_combinations**: dictionary indicating the combinations of sub-monthly and sub-daily periods (i.e., day types and time intervals) that are to be considered for the mid-term permanence score and metrics computation, for each month betwen **start_month** and **end_month**. The structure is as follows (a full example can be in [Configuration example](#configuration-example)):
   - The keys of the dictionary must be one of the possible day type values surrounded by quotes:
     - `"all"`: every day of the month.
     - `"workdays"`: every day of the month that does not belong to the weekend and is not a holiday in the country of study.
     - `"holidays"`: every day of the month that is a holiday in the country of study.
     - `"weekends"`: every day of the month that is part of the weekend.
     - `"mondays"`: every Monday of the month.
     - `"tuesdays"`: every Tuesday of the month.
     - `"wednesdays"`: every Wednesday of the month.
     - `"thursdays"`: every Thursday of the month.
     - `"fridays"`: every Friday of the month.
     - `"saturdays"`: every Saturday of the month.
     - `"sundays"`: every Sunday of the month.
   - The value assigned to a key must be a non-empty list surrounded by square brackets containing some of the possible time interval values:
     - `"all"`: all time slots of the day.
     - `"night_time"`: time slots of the day contained in the hour interval defined by **night_time_start** and **night_time_end**.
     - `"working_hours"`: time slots of the day contained in the hour interval defined by **working_hours_start** and **working_hours_end**.
     - `"evening_time"`: time slots of the day contained in the hour interval defined by **evening_time_start** and **evening_time_end**.

## Time interval additional information
There are some nuances and restrictions related to the definition of the different time intervals and the **day_start_hour** parameter:
 - By definition, a time interval will belong to the date that contains its start hour. See the following example:
   - Suppose that **day_start_hour** has been set to `4`, so that the day "starts" at 04:00.
   - Suppose that **night_time_start** has been set to `20:15` and **night_time_end** has been set to `06:30`. Then, the night time interval starts at 20:15 in the evening of some day $D$, crosses midnight, and ends at 06:30 in the morning of the following day $D+1$.
   - In this case, the start of the time interval, 20:15, is between the 04:00 of day $D$ and the 04:00 of day $D+1$. Then, this time interval will be assigned to the date $D$.

 - The following configuration is not allowed for the time intervals *night_time*, *working_hours*, and *evening_time*:
   - **day_start_hour** is different from `0`, and
   - the start of the interval is between 00:00 (exclusive) and **day_start_hour** (exclusive), and
   - the end of the interval is between 00:00 (exclusive) and the start of the interval (exclusive).

    Example of a not-allowed configuration under this restriction:
   - **day_start_hour** is `4`, that is, 04:00.
   - **night_time_start** is `03:00`.
   - **night_time_end** is `01:15`.
 - The following configuration is not allowed for the time intervals *working_hours* and *evening_time*:
   - the start of the interval is between 00:00 (inclusive) and **day_start_hour** (exclusive)
   - the end of the interval is later than **day_start_hour** (exclusive).

    Example of a not-allowed configuration under this restriction:
    - **day_start_hour** is `4`, that is, 04:00.
    - **working_hours_start** is `03:00`.
    - **working_hours_end** is `18:00`.


## Configuration example

```ini
[Spark]
session_name = MidtermPermanenceScore

[MidtermPermanenceScore]

start_month = 2023-02
end_month = 2023-03

before_regularity_days = 7
after_regularity_days = 7
day_start_hour = 4  # at what time the day starts, e.g. day gos from 4AM Mon to 4AM Tue

country_of_study = ES

night_time_start = 18:00
night_time_end = 08:00

working_hours_start = 08:00
working_hours_end = 17:00

evening_time_start = 16:00
evening_time_end = 22:00

weekend_start = 6
weekend_end = 7

period_combinations = {
    "all": ["all", "night_time", "evening_time", "working_hours"],
    "workdays": ["night_time", "working_hours"],
    "holidays": ["all", "night_time"],
    "weekends": ["all", "night_time"],
    "mondays": ["all"]
    }
```