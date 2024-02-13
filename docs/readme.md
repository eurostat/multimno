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

## Setup
The code stored in this repository is aimed to be executed in a PySpark compatible cluster. For an easy deployment in local environments, configuration for creating a docker container with all necessary dependencies is included in the `.devcontainer` folder. This allows users to execute the code
in an isolated environment with all requirements and dependencies installed. 

### Docker installation
Official guide: [Click here](https://docs.docker.com/engine/install/)


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
spark-submit multimno/hello_world.py
```

The hello world application will read a geoparquet file containing three geometries and will union all of them into a single geometry. The output will be a HTML file: *[sample_data/output/census.html](sample_data/output/census.html)* where the single geometry (corresponding to the Basque Country) can be seen.


### Try out the code
Configuration for executing a demo pipeline is given in the file: *[pipe_configs/pipelines/pipeline.json](pipe_configs/pipelines/pipeline.json)*
This file contains the order of the execution of the pipeline components and references to its configuration files.

```bash
python multimno/orchestrator.py pipe_configs/pipelines/pipeline.json
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
*TBD*