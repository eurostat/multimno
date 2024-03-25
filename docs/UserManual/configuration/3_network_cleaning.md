---
title: NetworkCleaning Configuration
weight: 3
---

# NetworkCleaning Configuration
To initialise and run the component two configs are used - `general_config.ini` and `network_cleaning.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Bronze]
...
network_data_bronze = ${Paths:bronze_dir}/mno_network
...

[Paths.Silver]
...
network_data_silver = ${Paths:silver_dir}/mno_network
network_syntactic_quality_metrics_by_column = ${Paths:silver_quality_metrics_dir}/network_syntactic_quality_metrics_by_column
...
```

The expected parameters in `network_cleaning.ini` are as follows:

- **latitude_min**: float, minimum accepted latitude (WGS84) for the latitude of cells in the input data. Values lower than this will be treated as out of bounds/range.
- **latitude_max**: float, maximum accepted latitude (WGS84) for the latitude of cells in the input data. Values higher than this will be treated as out of bounds/range.
- **longitude_min**: float, minimum accepted longitude (WGS84) for the longitude of cells in the input data. Values lower than this will be treated as out of bounds/range.
- **longitude_max**: float, minimum accepted longitude (WGS84) for the longitude of cells in the input data. Values higher than this will be treated as out of bounds/range.
- **cell_type_options**: comma-separated list of strings, this parameter indicates the accepted values in the `cell_type_options` field. Other values will be treated as out of bounds/range. Example: `macrocell, microcell, picocell`.
- **data_period_start**: string, format should be the one specified `data_period_format` (e.g., `2023-01-01` for `%Y-%m-%d`), the first date for which data will be processed by the component. All dates between this one and the specified in `data_period_end` will be processed (both inclusive).
- **data_period_end**: string, format should be "yyyy-MM-dd" (e.g., `2023-01-09` for `%Y-%m-%d`), the last date for which data will be processed by the component. All dates between the specified in `data_period_start` and this one will be processed (both inclusive).
- **data_period_format**: string, it indicates the format expected in `data_period_start` and `data_period_end`. For example, use `%Y-%m-%d` for the usual "2023-01-09" format separated by `-`.
- **valid_date_timestamp_format**: string, the timestamp format that is expected to be in the input network data and that will be parsed with PySpark using thiis format. Example: `yyyy-MM-dd'T'HH:mm:ss`

## Configuration example

```ini
[Spark]
session_name = NetworkCleaning

[NetworkCleaning]
# Bounding box
latitude_min = 40.352
latitude_max = 40.486
longitude_min = -3.751
longitude_max = -3.579

cell_type_options = macrocell, microcell, picocell

# Left- and right-inclusive date range for the data to be read
data_period_start = 2023-01-01
data_period_end = 2023-01-09
data_period_format = %Y-%m-%d

valid_date_timestamp_format = yyyy-MM-dd'T'HH:mm:ss

```