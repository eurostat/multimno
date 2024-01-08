# MultiMNO

This repository contains code that processes MNO Data to generate population and mobility insights.


- [MultiMNO](#multimno)
  - [Setup](#setup)
    - [Docker installation](#docker-installation)
  - [Local Execution](#local-execution)
    - [Docker image creation](#docker-image-creation)
    - [Docker container creation](#docker-container-creation)
    - [Hello world](#hello-world)
    - [Try out the code](#try-out-the-code)
    - [Clean up](#clean-up)
  - [Production Deployment](#production-deployment)
  - [Developer Guide](#developer-guide)
    - [Configurate docker container](#configurate-docker-container)
    - [Start dev environment](#start-dev-environment)
    - [Hello world](#hello-world-1)
      - [Python execution](#python-execution)
      - [Jupyter execution](#jupyter-execution)
    - [Launching a single component](#launching-a-single-component)
    - [Launching a pipeline](#launching-a-pipeline)
    - [Launching a spark history server](#launching-a-spark-history-server)
    - [Code Linting](#code-linting)

## Setup
The code stored in this repository is aimed to be executed in a PySpark compatible cluster. For an easy deployment in local environments, configuration for creating a docker container with all necessary dependencies is included in the `.devcontainer` folder. This allows users to execute the code
in an isolated environment with all requirements and dependencies installed. 

### Docker installation
Official guide: https://docs.docker.com/engine/install/



## Local Execution



### Docker image creation

Execute the following command:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env build
```

### Docker container creation
Create a container and start a shell session in it with the commands:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env up -d
docker exec -it multimno_dev_container bash
```

### Hello world
To test that the system has been correctly set try the hello world app with:

```bash
spark-submit src/hello_world.py
```

The hello world application will read a geoparquet file containing three geometries and will union all of them into a single geometry. The output will be a HTML file: *[sample_data/output/census.html](sample_data/output/census.html)* where the single geometry (corresponding to the Basque Country) can be seen.


### Try out the code
Configuration for executing a demo pipeline is given in the file: *[pipe_configs/pipelines/pipeline.json](pipe_configs/pipelines/pipeline.json)*
This file contains the order of the execution of the pipeline components and references to its configuration files.

```bash
python src/orchestrator.py pipe_configs/pipelines/pipeline.json
```

This demo will create synthetic Event data and clean it under the path *[sample_data/lakehouse](sample_data/lakehouse)*   

Synthetic event data will be created in: *[sample_data/lakehouse/bronze/mno_events](sample_data/lakehouse/bronze/mno_events)*  

Cleaned event data and the quality insights will be created in: *[sample_data/lakehouse/silver/mno_events](sample_data/lakehouse/bronze/mno_events)*  

A jupyter notebook is given for the results visualization. To use it, start a jupyterlab session with:
```bash
jl
```
Then go to http://localhost:${JL_PORT}/lab
  * ***JL_PORT** was defined in the *[.devcontainer/.env](.devcontainer/.env)* file.*  

For example :http://localhost:8888/lab

Then open the notebook: *[notebooks/demo_visualization.ipynb ](notebooks/demo_visualization.ipynb )* and execute all cells.

### Clean up
Exit the terminal with:

Ctrl+D or writing `exit`

Delete the container created with:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env down
```

## Production Deployment
TBD


## Developer Guide

The repository contains a devcontainer configuration compatible with VsCode. This configuration will create a docker container with all the necessary libraries and configurations to develop and execute the source code. 

### Configurate docker container

Edit the *[.devcontainer/.env](.devcontainer/.env)* file:

```ini
# ------------------- Docker Build parameters -------------------
PYTHON_VERSION=3.11 # Python version.
JDK_VERSION=17 # Java version.
SPARK_VERSION=3.4.1 # Spark/Pyspark version.
SEDONA_VERSION=1.5.0 # Sedona
GEOTOOLS_WRAPPER=28.2 # Sedona dependency

# ------------------- Docker run parameters -------------------
CONTAINER_NAME=multimno_dev_container # Container name.
DATA_DIR=../sample_data # Path of the host machine to the data to be used within the container.
SPARK_LOGS_DIR=../sample_data/logs # Path of the host machine to where the spark logs will be stored.
JL_PORT=8888 # Port of the host machine to deploy a jupyterlab.
JL_CPU=4 # CPU cores of the container.
JL_MEM=16g # RAM of the container.
```


### Start dev environment 

In VsCode: **F1 -> Dev Containers: Rebuild and Reopen in container**

### Hello world 

#### Python execution
Try the hello world app of section: [Hello world](#hello-world)

#### Jupyter execution
Open the hello world jupyter notebook stored in *[notebooks/hello_world.ipynb](notebooks/hello_world.ipynb)* with VsCode and execute all cells.  
*The Jupyter extension is installed automatically in the devcontainer.*

### Launching a single component
In a terminal execute the command:
```bash
spark-submit src/main.py <component_id> <path_to_general_config> <path_to_component_config>
```

Example:
 ```bash
spark-submit src/main.py SyntheticEvents pipe_configs/configurations/general_config.ini pipe_configs/configurations/synthetic_events/synth_config.ini 
```

### Launching a pipeline
In a terminal execute the command:
```bash
python src/orchestrator.py <pipeline_json_path>
```

Example
```
python src/orchestrator.py pipe_configs/pipelines/pipeline.json 
```

### Launching a spark history server
The history server will access SparkUI logs stored at the path ${SPARK_LOGS_DIR} defined in the *[.devcontainer/.env](.devcontainer/.env)* file.

Starting the history server
```bash
start-history-server.sh 
```
Accesing the history server
* Go to the address http://localhost:18080

### Code Linting

The python code generated shall be formatted with autopep8. For formatting all source code execute the
following command:

```bash
black -l 120 src
```


