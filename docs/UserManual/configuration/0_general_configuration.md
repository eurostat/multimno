---
title: General Configuration
weight: 0
---

# General Configuration

The general configuration file contains transversal settings for all the pipeline. It is an INI file composed of four main sections:

**Logging**: Section which contains the logger settings.

**Paths**: Section containing the definition of all paths to be used.

**Spark**: Apache Spark configuration values. Parameters defined in this section will be used to create the spark session. Values supported are based in: Configuration - Spark 3.5.1 Documentation (apache.org). As an exception the parameter session_name has been included which identifies the name of the spark session.

**General**: Parameters that may be useful for all components in the pipeline. 

Example:

```ini
[Logging]
level = INFO
format= %(asctime)-20s %(message)s
datefmt = %y-%m-%d %H:%M:%S

[Paths]
# Main paths
home_dir = /opt/data
lakehouse_dir = ${Paths:home_dir}/lakehouse
# Lakehouse
landing_dir = ${Paths:lakehouse_dir}/landing
bronze_dir = ${Paths:lakehouse_dir}/bronze
silver_dir = ${Paths:lakehouse_dir}/silver
gold_dir = ${Paths:lakehouse_dir}/gold

[Spark]
# parameters in this section are passed to Spark except session_name 
session_name = MultiMNO
spark.master = local[*]
spark.driver.host = localhost

[General]
start_date = 2023-01-01
end_date = 2023-01-30
```