
# Developer Guide

The repository contains a devcontainer configuration compatible with VsCode. This configuration will create a docker container with all the necessary libraries and configurations to develop and execute the source code. 

- [Developer Guide](#developer-guide)
  - [Configurate docker container](#configurate-docker-container)
  - [Start dev environment](#start-dev-environment)
  - [Hello world](#hello-world)
    - [Python execution](#python-execution)
    - [Jupyter execution](#jupyter-execution)
  - [Launching a single component](#launching-a-single-component)
  - [Launching a pipeline](#launching-a-pipeline)
  - [Launching a spark history server](#launching-a-spark-history-server)
  - [Testing](#testing)
    - [See coverage in IDE (VsCode extension)](#see-coverage-in-ide-vscode-extension)
  - [Code Linting](#code-linting)
  - [Code Documentation](#code-documentation)
    - [Update mkdocs index with readme](#update-mkdocs-index-with-readme)
    - [Generate Static documentation](#generate-static-documentation)
    - [Documentation server (Mkdocs)](#documentation-server-mkdocs)


## Configurate docker container

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


## Start dev environment 

In VsCode: **F1 -> Dev Containers: Rebuild and Reopen in container**

## Hello world 

### Python execution
Try the hello world app of section: [Hello world](#hello-world)

### Jupyter execution
Open the hello world jupyter notebook stored in *[notebooks/hello_world.ipynb](notebooks/hello_world.ipynb)* with VsCode and execute all cells.  
*The Jupyter extension is installed automatically in the devcontainer.*

## Launching a single component
In a terminal execute the command:  
  
```bash
spark-submit multimno/main.py <component_id> <path_to_general_config> <path_to_component_config> 
```

Example:  
  
```bash
spark-submit multimno/main.py SyntheticEvents pipe_configs/configurations/general_config.ini pipe_configs/configurations/synthetic_events/synth_config.ini 
```

## Launching a pipeline
In a terminal execute the command:  

```bash
python multimno/orchestrator.py <pipeline_json_path>
```

Example:  

```
python multimno/orchestrator.py pipe_configs/pipelines/pipeline.json 
```

## Launching a spark history server
The history server will access SparkUI logs stored at the path ${SPARK_LOGS_DIR} defined in the *[.devcontainer/.env](.devcontainer/.env)* file.

Starting the history server
```bash
start-history-server.sh 
```
Accesing the history server
* Go to the address http://localhost:18080

## Testing

### See coverage in IDE (VsCode extension)
1) Generate the coverage report (xml|lcov)
```bash
pytest --cov-report="xml" --cov=multimno tests/test_code/
```
2) Install the extension: Coverage Gutters
3) Right click and select Coverage Gutters: Watch

*Note: You can see the coverage percentage at the bottom bar*

## Code Linting

The python code generated shall be formatted with autopep8. For formatting all source code execute the
following command:

```bash
black -l 120 multimno tests/test_code/
```

## Code Documentation

### Update mkdocs index with readme

```bash
cp README.md docs/readme.md
```

### Generate Static documentation

```bash
mkdocs build --no-directory-urls
```

The documentation will be generated under the **site** directory.

### Documentation server (Mkdocs)

A code documentation can be deployed using mkdocs backend. 

1) Create documentation
```bash
./scripts/generate_docs.sh
```
2) Launch doc server

```bash
mkdocs serve
```
and navigate to the address: http://127.0.0.1:8000


