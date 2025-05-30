---
title: EventCleaning Configuration
weight: 2
---

# EventCleaning Configuration
To initialise and run the component two configs are used - general_config.ini and event_cleaning.ini.  In general_config.ini to execute Event Cleaning component specify all paths to its four corresponding data objects (input + output). Example: 


```ini
[Paths.Bronze]
event_data_bronze = ${Paths:bronze_dir}/mno_events

[Paths.Silver]
event_data_silver = ${Paths:silver_dir}/mno_events
event_syntactic_quality_metrics_by_column = ${Paths:silver_dir}/event_syntactic_quality_metrics_by_column
event_syntactic_quality_metrics_frequency_distribution = ${Paths:silver_dir}/event_syntactic_quality_metrics_frequency_distribution
```

In event_cleaning.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **data_folder_date_format** - string, to what string format convert dates so they match the naming of input data folders (it is expected that input data is divided into separate folders for each date of research period). Example: if you know that data for 2023-01-01 is stored in f"{bronze_event_path}/20230101", then the format to convert 2023-01-01 date to 20230101 string using strftimewill be %Y%m%d

- **spark_data_folder_date_format** - string, as for data_folder_date_format it depends on folder’s naming pattern of input data but since datetime patterns in pyspark and strftime differ, it is a separate config param. Used to convert string to datetype when creating date column in frequency distribution table 

- **timestamp_format** - str, expected string format of timestamp column when converting it to timestamp type

- **do_bounding_box_filtering**- boolean, True/False, decides whether to apply bounding box filtering


- **bounding_box** - dictionary, with following keys 'min_lon', 'max_lon', 'min_lat', and 'max_lat' and integer/float values, to specify coordinates of bounding box, within which records should fall, make sure that records and bounding box are in the same src 

- **number_of_partitions** - an integer, that determines the value of the modulo operator. This value will determine the number expected partitions as to the last partitioning column user_id_modulo. This value does not affect the number of folders in terms of other partitioning columns (day, month, year).
- **do_user_id_rehashing** - boolean, True/False, decides whether to apply user_id rehashing before modulo partitioning. If True, user_id will be hashed using SHA2 algorithm and then modulo operation will be applied to the hash value. This is useful for ensuring that the user_id values are uniformly distributed across the partitions.

## Configuration example

```ini
[EventCleaning]
data_period_start = 2023-01-01
data_period_end = 2023-01-05
data_folder_date_format = %Y%m%d
spark_data_folder_date_format = yyyyMMdd
timestamp_format = yyyy-MM-dd'T'HH:mm:ss
do_bounding_box_filtering = True
do_same_location_deduplication = True
bounding_box = {
    'min_lon': -180,
    'max_lon': 180,
    'min_lat': -90,
    'max_lat': 90
    }
number_of_partitions = 256
do_user_id_rehashing = False
```
