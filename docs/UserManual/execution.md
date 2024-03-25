---
title: Execution
weight: 3
---

# Execution

The multimno software is a python application that launches a single component with a given configuration. 
This atomic design allows the application to be integrated with multiple orchestration software. At the moment
a python script called `orchestrator_multimno.py` is provided which will execute a pipeline of components sequentially
using `spark-submit` commands.

## Single component execution

The entrypoint of the application is a main.py which receives the following positional parameters:
- **component_id**: Id of the component that will be launched.
- **general_config_path**: Path to the general configuration file of the application.
- **component_config_path**: Path to the component configuration file.

```bash
multimno/main.py <component_id> <general_config_path> <component_config_path>
```

Example:

```bash
multimno/main.py InspireGridGeneration pipe_configs/configurations/general_config.ini pipe_configs/configurations/grid/grid_generation.ini
```

## Pipeline execution
For executing a pipeline the `orchestrator_multimno.py` script shall be used which takes a path to a json file
as its only parameter. This json file, shall be defined as a file that glues all the configuration files 
and defines the execution order of the components. The structure is as follows:

- **general_config_path:** Path to the general configuration file
- **spark_submit_args:** List containing arguments that will be passed to the *spark-submit* command. It can be empty.
- **pipeline:** List containing the order in which the components will be executed. Each item is composed of the values:
    - **component_id:** Id of the component to be executed.
    - **component_config_path:** Path to the component configuration file.

Example:
```json
{
    "general_config_path": "/opt/dev/pipe_configs/configurations/general_config.ini",
    "spark_submit_args": [
        "--master=spark://spark-master:7077",
        "--packages=org.apache.sedona:sedona-spark-3.5_2.12:1.5.1,org.datasyslab:geotools-wrapper:1.5.1-28.2"
    ],
    "pipeline": [
        {
            "component_id": "SyntheticEvents",
            "component_config_path": "/opt/dev/pipe_configs/configurations/synthetic_events/synth_config.ini"
        },
        {
            "component_id": "EventCleaning",
            "component_config_path": "/opt/dev/pipe_configs/configurations/event/event_cleaning.ini"
        }
    ]
}
```

Configuration for executing a demo pipeline is given in the file: `pipe_configs/pipelines/pipeline.json`
This file contains the order of the execution of the pipeline components and references to its configuration files.

```bash
./orchestrator_multimno.py pipe_configs/pipelines/pipeline.json
```

This demo will process MNO Event & Network data cleaning it. At the same time it will generate quality metrics
in both of these processes. Then, it will process the cleaned data until the generation of the continuous time 
segmentation and daily permanence score indicators.

> If using the docker setup, all data will be stored under the path `/opt/data/lakehouse`. 