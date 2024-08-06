---
title: DeviceActivityStatistics
weight: 9
---

# DeviceActivityStatistics Configuration
To initialise and run the component two configs are used - general_config.ini and device_activity_statistics.ini.  In general_config.ini to execute Device Activity Statistics component specify all paths to its three corresponding data objects (input + output). The local timezone must also be specified in the general config. Example: 


```ini
[Timezone]
local_timezone = UTC

[Paths.Silver]
# Data
network_data_silver = ${Paths:silver_dir}/mno_network

device_activity_statistics = ${Paths:silver_dir}/device_activity_statistics
```

In device_activity_statistics.ini parameters are as follows: 

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-01), the date from which start Event Cleaning

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-05), the date till which perform Event Cleaning

- **clear_destination_directory** - boolean, whether to empty the destination directory before running or not


## Configuration example

```ini
[DeviceActivityStatistics]
clear_destination_directory = True
data_period_start = 2023-01-01
data_period_end = 2023-01-04
```
