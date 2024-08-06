---
title: Setup Guide
weight: 1
---
# Setup Guide

The **multimno** software is a python application using the [PySpark library](https://spark.apache.org/docs/latest/api/python/index.html#:~:text=PySpark%20is%20the%20Python%20API,for%20interactively%20analyzing%20your%20data.) to harness the power of Apache Spark, a fast and general-purpose cluster-computing system. PySpark provides an interface for Apache Spark in Python, enabling developers to utilize Spark's high-level APIs and distributed computing capabilities while working in the Python programming language. The Spark framework is critical to this application as it handles the distribution of data and computation across the cluster, ensures fault tolerance, and optimizes execution for performance gains. Deployment of a PySpark application can be done on a single node (local mode), where Spark runs on a single machine for simplicity and ease of development or testing. Alternatively, for production and scaling, it can be deployed on a cluster (cluster mode) comprising multiple machines, either on-premises or in the cloud, where Spark distributes tasks across the nodes, allowing for parallel processing and efficient handling of large-scale data workloads.
There are two ways of setting up a system for executing the source code:  
  1) Building the docker image provided. (Recommended for local executions)  
  2) Installing and setting up all required system libraries.  

## Docker setup

A Dockerfile is provided to build a docker image with all necessary dependencies for the code execution.

### Installing docker software

To use the docker image it is necessary to have the docker engine installed. Please follow the official docker 
guide to set it up in your system:
-  Official guide: [Click here](https://docs.docker.com/engine/install/)

### Docker image creation

Execute the following command:
```bash
docker build -t multimno:1.0-prod --target=multimno-prod .
```

### Docker container creation

#### Run an example pipeline within a container
```bash
docker run --rm --name=multimno-container -v "${PWD}/sample_data:/opt/data" -v "${PWD}/pipe_configs:/opt/app/pipe_configs" multimno:1.0-prod pipe_configs/pipelines/pipeline.json
```

#### Run a container in interactive mode
```bash
docker run -it --name=multimno-container -v "${PWD}/sample_data:/opt/data" -v "${PWD}/pipe_configs:/opt/app/pipe_configs" --entrypoint=bash multimno:1.0-prod 
```

After performing this command your shell(command-line) will be inside the container and you can perform 
the [execution steps](./execution.md) to try out the code.

---

### Clean up
Exit the terminal with:

Ctrl+D or writing `exit`

Delete the container created with:
```bash
docker rm multimno-container
```

Delete the docker image with:
```bash
docker rmi multimno:1.0-prod
```

### Docker Lite version

As the multimno software is a python application designed to be executed in a Spark cluster, a lightweight Dockerfile called `Dockerfile-lite` is given for execution of the software in existing Spark clusters.

This docker image contains only minimum requirements to launch the application against an existing spark cluster. The image is a ubuntu:22.04 with python 3.10, jdk 17 and the required python dependencies.

#### Build lite image

Execute the following command:
```bash
docker build -t multimno_lite:1.0 -f ./Dockerfile-lite .
```

#### Create lite container

```bash
docker run -it --name=multimno-lite-container -v "${PWD}/pipe_configs:/opt/app/pipe_configs" multimno_lite:1.0 bash
```

#### Configuration

**spark-submit args**

As explained in the [execution guide](execution.md), the entrypoint for the pipeline execution: `orchestrator_multimno.py`, performs `spark-submit` commands. To define `spark-submit` arguments edit the `spark_submit_args` variable in the pipeline.json.

- Spark submit documentation: https://spark.apache.org/docs/latest/submitting-applications.html

**Spark Configuration**

Edit the `[Spark]` section in the general_configuration file to define Spark session configuration parameters.

- Spark configuration documentation: https://spark.apache.org/docs/latest/configuration.html

**Python Version**
A requirement of a pyspark application is that the python version must be alligned for all the cluster. As the Dockerfile-Lite uses **python3.10 the Spark cluster must have this python version alligned.**

- Python package management: https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html

**Python dependencies**
The application uses Apache Sedona and so it will need the Sedona jars as well as python dependencies installed in the Spark cluster. Please refer to the Apache Sedona official documentation: 

- Python setup: https://sedona.apache.org/1.5.1/setup/install-python/

- Spark Cluster: https://sedona.apache.org/1.5.1/setup/cluster/

**Lite Configuration example**
An example of configuration of an execution with the lite image is given in the files: `pipe_configs/pipelines/test_production.json` and `pipe_configs/configurations/general_config_production.ini`

#### Execution lite

Execute as defined in the [execution guide.](execution.md)

## Software setup

The software is aimed to be executed in a Linux OS. It is recommended to use Ubuntu 22.04 LTS but these steps should also work in MAC OS 12.6(or superior) and in Windows 11 with WSL2 and setting up as the distro of WSL Ubuntu 22.04.

### Install system libs

```bash
sudo apt-get update && \
    sudo apt-get install -y --no-install-recommends \
      sudo \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      openssh-client openssh-server \
      gdal-bin \
      libgdal-dev \
      ssh
```


### Download Spark source code

```bash
SPARK_VERSION=3.5.1
export SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
mkdir -p ${SPARK_HOME}
cd ${SPARK_HOME}
wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
  && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
  && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz
export PATH="${PATH}:$SPARK_HOME/bin:$SPARK_HOME/sbin"
```

### Install python requirements

```bash
pip install --upgrade pip
pip install -r resources/requirements/requirements.in
pip install -r resources/requirements/dev_requirements.in
```

> You can use a [virtualenv](https://virtualenv.pypa.io/en/latest/) for avoiding conflicts with other python libraries.

### Install Spark dependencies

```bash
SCALA_VERSION=2.12
SEDONA_VERSION=1.5.1
GEOTOOLS_WRAPPER_VERSION=28.2
chmod +x ./resources/scripts/install_sedona_jars.sh
./resources/scripts/install_sedona_jars.sh ${SPARK_VERSION} ${SCALA_VERSION} ${SEDONA_VERSION} ${GEOTOOLS_WRAPPER_VERSION} 
```

### Setup spark configuration

```bash
cp conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
cp conf/log4j2.properties "$SPARK_HOME/conf/log4j2.properties"
```
