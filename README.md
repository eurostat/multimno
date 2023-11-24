# MultiMNO

The code stored in this repository is aimed to be executed in a PySpark compatible cluster. For an easy deployment in local environments, configuration for creating a docker container with all necessary dependencies is included in the `.devcontainer` folder. 

## Execution

### Local
Create a container and start a shell session in it with the commands:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env up -d --build
docker exec -it multimno_dev_container bash
```

Try a hello world app with:

```bash
spark-submit src/hello_world.py
```

Destroy the environment
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env down
```

### Remote cluster
TBD


## Developer Guide

The repository contains a devcontainer configuration compatible with VsCode. This configuration will create a docker container with all the necessary libraries and configurations to develop and execute the source code. 

### Configurate docker container

Edit the `.env` file located in `.devcontainer/.env`

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

In VsCode: F1 -> Dev Containers: Rebuild and Reopen in container

### Hello world 

Try a hello world app with:

```bash
spark-submit src/hello_world.py
```

Try a hello world jupyter notebook stored in `notebooks/hello_world.ipynb` with:  
* Opening it with VsCode within the devcontainer.
* Starting a jupyterlab service with the command within the container and going to the address http://localhost:8888/lab.
```bash
jl
```
