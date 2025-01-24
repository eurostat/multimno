---
title: General Configuration
weight: 0
---

# General Configuration

The general configuration file contains transversal settings for all the pipeline. It is an INI file composed of three main sections:

**General**: Section containing configuration values which are pipeline wide. 

**Logging**: Section which contains the logger settings.

**Paths**: Section containing the definition of all paths to be used.

**Spark**: Apache Spark configuration values. 

## General section
- **local_timezone:** str, Specifies the time zone in IANA format (e.g., Europe/Madrid) to represent regional time settings.
- **local_mcc:** int, MCC code of the country of study.


## Logging Section

- **level:** The minimum logging level for console output. Messages with a level equal to or higher than this will be output to the console.
- **format:** The format of the log messages for console output.
- **datefmt:** The format of the date/time in the log messages for console output.
- **file_log_level:** The minimum logging level for file output. Messages with a level equal to or higher than this will be written to the log file.
- **file_format:** The format of the log messages for file output.
- **report_path:** The path where the log file will be written.

## Paths Section
- **home_dir:** The main directory for the application. 
- **lakehouse_dir:** The directory for the "lakehouse" data.
- **exports_dir:** The directory where exported data is stored.
- **landing_dir:** The directory for the "landing" data within the lakehouse.
- **bronze_dir:** The directory for the "bronze" data within the lakehouse.
- **silver_dir:** The directory for the "silver" data within the lakehouse.
- **gold_dir:** The directory for the "gold" data within the lakehouse.
- **app_log_dir:** The directory where application logs are stored.
- **spark_log_dir:** The directory where Spark logs are stored (Output for the Spark History Server).
- **silver_quality_metrics_dir:** The directory for quality metrics of the "silver" data.
- **silver_quality_warnings_dir:** The directory for quality warnings of the "silver" data.
- **gold_quality_warnings_dir:** The directory for quality warnings of the "gold" data.
- **spark_checkpoint_dir:** The directory for Spark checkpoints.

All the [Paths] subsections (Paths.Landing, Paths.Bronze, Paths.Silver, Paths.Gold), contain the different paths for the output of the pipeline components. It is recommended to use the default values as all this paths are relative to the ones specified in the [Paths] section.

## Spark Section
Parameters defined in this section will be used to create the spark session. Values supported are based in: [Spark configuration](https://spark.apache.org/docs/latest/configuration.html). As an exception, the parameter `session_name` has been included which identifies the name of the spark session.



Example:

```ini
[General]
local_timezone = Europe/Madrid
local_mcc = 214

[Logging]
level = INFO
format= %(asctime)-20s %(message)s
datefmt = %y-%m-%d %H:%M:%S
file_log_level = INFO
file_format = %(asctime)-20s |%(name)s| %(levelname)s: %(message)s
report_path = /opt/data/app_log

[Paths]
# Main paths
home_dir = /opt/data
lakehouse_dir = ${Paths:home_dir}/lakehouse
exports_dir = ${Paths:home_dir}/exports
# Lakehouse
landing_dir = ${Paths:lakehouse_dir}/landing
bronze_dir = ${Paths:lakehouse_dir}/bronze
silver_dir = ${Paths:lakehouse_dir}/silver
gold_dir = ${Paths:lakehouse_dir}/gold
# Logs dir
app_log_dir = ${Paths:home_dir}/log
spark_log_dir = ${Paths:home_dir}/spark_log
# Quality metrics dir
silver_quality_metrics_dir = ${Paths:silver_dir}/quality_metrics
# Quality warnings dir
silver_quality_warnings_dir = ${Paths:silver_dir}/quality_warnings
# Quality graphs
gold_quality_warnings_dir = ${Paths:gold_dir}/quality_warnings
# spark
spark_checkpoint_dir = ${Paths:home_dir}/spark/spark_checkpoint

[Spark]
# parameters in this section are passed to Spark except session_name 
session_name = MultiMNO
spark.master = local[*]
spark.driver.host = localhost
```