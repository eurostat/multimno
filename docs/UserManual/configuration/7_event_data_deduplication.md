---
title: EventDeduplication Configuration
weight: 7
---

# EventDeduplication Configuration
To initialise and run the component two configs are used - `general_config.ini` and `event_deduplication.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). Example: 


```ini
[Paths.Silver]
event_data_silver = ${Paths:silver_dir}/mno_events
event_data_silver_deduplicated = ${Paths:silver_dir}/mno_events_deduplicated
event_deduplicated_quality_metrics_by_column = ${Paths:silver_quality_metrics_dir}/event_deduplicated_quality_metrics_by_column
event_deduplicated_quality_metrics_frequency_distribution = ${Paths:silver_quality_metrics_dir}/event_deduplicated_quality_metrics_frequency_distribution
```

In event_deduplication.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start processing

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform processing

- **spark_data_folder_date_format** - string, as for data_folder_date_format it depends on folder’s naming pattern of input data but since datetime patterns in pyspark and strftime differ, it is a separate config param. Used to convert string to datetype when creating date column in frequency distribution table 

- **clear_destination_directory** - boolean, if True, the destination directory will be cleared before writing new data to it

## Configuration example

```ini
[EventDeduplication]
data_period_start = 2023-01-01
data_period_end = 2023-01-15
spark_data_folder_date_format = yyyyMMdd
clear_destination_directory = True
```