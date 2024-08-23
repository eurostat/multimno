---
title: Execution
weight: 3
---

# Execution

The multimno software is a python application that launches a single component with a given configuration. 
This atomic design allows the application to be integrated with multiple orchestration software. 

At the moment a python script called `orchestrator_multimno.py` is provided which will execute a 
pipeline of components sequentially using `spark-submit` commands.

The execution process can be divided into four steps:

## 1. Setting - Input Data

The following input data is required to execute the multimno software addtionally from the MNO Data:

#### National Holidays data

National holiday data is required to execute the software. This data must be in parquet format and contain the following
schema:

| **Column** | **Format** | **Value**                                  |
| ---------- | ---------- | ------------------------------------------ |
| iso2       | str        | Country code in iso2 format                |
| date       | date       | Date of the festivity in yyyy-mm-dd format |
| name       | str        | Festivity description                      |


Example:
| **iso2** | **date**   | **name**           |
| -------- | ---------- | ------------------ |
| ES       | 2022-01-01 | Año Nuevo          |
| ES       | 2022-01-06 | Epifanía del Señor |
| ES       | 2022-04-15 | Viernes Santo      |

The path to this data must be specified in the `holiday_calendar_data_bronze` variable under the section `[Paths.Bronze]` 
in the general_configuration.ini file of the apllication.

## 2. Pipeline definition

The pipeline is defined as a json file that glues all the configuration files 
and defines the sequential execution order of the components. The structure is as follows:

- **general_config_path:** Path to the general configuration file
- **spark_submit_args:** List containing arguments that will be passed to the *spark-submit* command. It can be empty.
- **pipeline:** List containing the order in which the components will be executed. Each item is composed of the values:
    - **component_id:** Id of the component to be executed.
    - **component_config_path:** Path to the component configuration file.

Example:
```json
{
    "general_config_path": "pipe_configs/configurations/general_config.ini",
    "spark_submit_args": [
        "--master=spark://spark-master:7077",
        "--packages=org.apache.sedona:sedona-spark-3.5_2.12:1.6.0,org.datasyslab:geotools-wrapper:1.6.0-28.2"
    ],
    "pipeline": [
        {
            "component_id": "InspireGridGeneration",
            "component_config_path": "pipe_configs/configurations/grid/grid_generation.ini"
        },
        {
            "component_id": "EventCleaning",
            "component_config_path": "pipe_configs/configurations/event/event_cleaning.ini"
        }
    ]
}
```

Configuration for executing a demo pipeline is given in the file: `pipe_configs/pipelines/pipeline.json`
This file contains the order of the execution of the pipeline components and references to its demo configuration files 
that are given as well in the repository.

## 3. Execution Configuration

Each component of the pipeline to be executed must be configured to the user desired settings. It is recommended to take 
the configurations defined in `pipe_configs/configurations` as a base and refine them using the [configuration guide](configuration/index.md).

### Spark configuration

**spark-submit args**

The entrypoint for the pipeline execution: `orchestrator_multimno.py`, performs `spark-submit` commands to execute each component of the pipeline as a Spark Job. To define `spark-submit` arguments edit the `spark_submit_args` variable in the pipeline.json. 
> This variable follows the same syntax as `spark-submit` arguments.

- Spark submit documentation: https://spark.apache.org/docs/latest/submitting-applications.html

**Spark Configuration**

To define Spark session specific configurations, edit the `[Spark]` section in the general_configuration file. If you want to change the Spark configuration for only one component in the pipeline, you can edit the the `[Spark]` section in the component configuration file which will override values defined in the general configuration file.


- Spark configuration documentation: https://spark.apache.org/docs/latest/configuration.html

## 4. Execution 

### Pipeline execution
For executing a pipeline the `orchestrator_multimno.py` entrypoint shall be used. This takes as input a path to a json file
with the pipeline definition as defined in the previous [Pipeline definition](execution.md#pipeline-definition) section. 

Example:
```bash
./orchestrator_multimno.py pipe_configs/pipelines/pipeline.json
```

!!! warning
    The `orchestrator_multimno.py` must be located in the same directory as the `main_multimno.py` file.

### Single component execution

If you want to only launch a component you can perform manually the spark-submit command from a terminal using the `main_multimno.py` entrypoint:

The entrypoint of the application is a main.py which receives the following positional parameters:
- **component_id**: Id of the component that will be launched.
- **general_config_path**: Path to the general configuration file of the application.
- **component_config_path**: Path to the component configuration file.

```bash
spark-submit multimno/main_multimno.py <component_id> <general_config_path> <component_config_path>
```

Example:

```bash
spark-submit multimno/main_multimno.py InspireGridGeneration pipe_configs/configurations/general_config.ini pipe_configs/configurations/grid/grid_generation.ini
```

