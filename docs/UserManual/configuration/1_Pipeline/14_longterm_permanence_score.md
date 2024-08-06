---
title: LongtermPermanenceScore Configuration
weight: 14
---

# LongtermPermanenceScore Configuration
To initialise and run the component two configs are used - `general_config.ini` and `longterm_permanence_score.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Silver]
...
midterm_permanence_score_data_siilver = ${Paths:silver_dir}/midterm_permanence_score
longterm_permanence_score_data_siilver = ${Paths:silver_dir}/longterm_permanence_score
...
```

The expected parameters in `longterm_permanence_score.ini` are as follows:
 - **start_month**: string, in `YYYY-MM` format, it indicates the first month to be included in the long-term permanence score computations and analyses. All months between **start_month** and **end_month**, both inclusive, will be considered in the long-term analyses. Example: `2023-01`.
 - **end_month**: string, in `YYYY-MM` format, it indicates the last month to be included in the long-term permanence score computations and analyses. All months between **start_month** and **end_month**, both inclusive, will be considered in the long-term analyses. Example: `2023-06`.
 - **winter_months**: comma-separated sequence of integers between 1 and 12, they indicate which months of the year belong to the *winter* season. 1 represents January, 2 represents February, ..., and 12 represents December. Example: `12, 1, 2`.
 - **spring_months**: comma-separated sequence of integers between 1 and 12, they indicate which months of the year belong to the *spring* season. 1 represents January, 2 represents February, ..., and 12 represents December. Example: `3, 4, 5`.
 - **summer_months**: comma-separated sequence of integers between 1 and 12, they indicate which months of the year belong to the *summer* season. 1 represents January, 2 represents February, ..., and 12 represents December. Example: `6, 7, 8`.
 - **autumn_months**: comma-separated sequence of integers between 1 and 12, they indicate which months of the year belong to the *autumn* season. 1 represents January, 2 represents February, ..., and 12 represents December. Example: `9, 10, 11`.
 - **period_combinations**: dictionary indicating the combinations of sub-yearly, sub-monthly and sub-daily periods (i.e., seasons, day types and time intervals) to consider in the long-term analyses. Each combination will result in the computation of indicators resulting from aggregating the data of months belonging to the particular season, day type and time interval defined by the combination. The structure is as follows (a full example can be found in [Configuration example](#configuration-example)):
    - The keys of the dictionary must be one of the possible season values:
        - `"all"`: every month between **start_month** and **end_month**, both inclusive, are considered in the aggregation.
        - `"winter"`: every month included in the **winter_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation. If this key appears then the **winter_months** list must contain at least one month.
        - `"spring"`: every month included in the **spring_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation. If this key appears then the **spring_months** list must contain at least one month.
        - `"summer"`: every month included in the **summer_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation. If this key appears then the **summer_months** list must contain at least one month.
        - `"autumn"`: every month included in the **autumn_months** list between **start_month** and **end_month**, both inclusive, are considered in the aggregation. If this key appears then the **autumn_months** list must contain at least one month.
    - The values assigned to a key is, in turn, another dictionary with the following structure:
        - The keys of this dictionary must be one of the possible day type values surrounded by quotes:
            - `"all"`
            - `"workdays"`
            - `"holidays"`
            - `"weekends"`
            - `"mondays"`
            - `"tuesdays"`
            - `"wednesdays"`
            - `"thursdays"`
            - `"fridays"`
            - `"saturdays"`
            - `"sundays"`
        - The value assigned to a key must be a non-empty list surrounded by square brackets containing some of the possible time interval values:
            - `"all"`
            - `"night_time"`
            - `"working_hours"`
            - `"evening_time"`
    
    The **period_combinations** example that appears in the [Configuration example](#configuration-example) would result in the computation of the long-term permanence metrics for the following combinations:

    | season | day type | time interval   |
    |--------|----------|-----------------|
    | all    | all      | all             |
    | all    | all      | night_time      |
    | winter | all      | all             |
    | spring | all      | all             |
    | spring | workdays | working_hours   |



    


## Configuration example

```ini
[Spark]
session_name = LongtermPermanenceScore

[LongtermPermanenceScore]
start_month = 2023-01
end_month = 2023-06

winter_months = 12, 1, 2
spring_months = 3, 4, 5
summer_months = 6, 7, 8
autumn_months = 9, 10, 11

period_combinations = {
    "all": {
        "all": ["all", "night_time"]
    },
    "winter": {
        "all": ["all"],
    },
    "spring": {
        "all": ["all"],
        "workdays": ["working_hours"]
    }
    }
```