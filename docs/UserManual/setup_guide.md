---
title: Setup Guide
weight: 1
---
# Setup Guide

## Introduction

The **multimno** software is a python application using the [PySpark library](https://spark.apache.org/docs/latest/api/python/index.html#:~:text=PySpark%20is%20the%20Python%20API,for%20interactively%20analyzing%20your%20data.) to harness the power of Apache Spark, a fast and general-purpose cluster-computing system. PySpark provides an interface for Apache Spark in Python, enabling developers to utilize Spark's high-level APIs and distributed computing capabilities while working in the Python programming language. The Spark framework is critical to this application as it handles the distribution of data and computation across the cluster, ensures fault tolerance, and optimizes execution for performance gains. Deployment of a PySpark application can be done in two ways:

1) **Cluster mode:** On a Cluster, where Spark distributes tasks across the nodes, allowing for parallel processing and efficient handling of large-scale data workloads.
**Recommended for production environments.**
2) **Local mode:** On a single node, where Spark runs on a single machine. **Recommended for development and testing environments.** 


## Cluster Mode

There are multiple ways of deploying a Spark cluster: Standalone, YARN managed, Kubernetes, Cloud managed services...  

This guide will not enter in the specific steps for deploying a Spark cluster and only explain the requirements 
and software installation steps in an existing cluster. All nodes in the cluster created shall satisfy the [OS libraries requirements](../system_requirements.md#os-libraries).
> Spark cluster mode official documentation: https://spark.apache.org/docs/latest/cluster-overview.html

### Package & deploy the code

There are two ways of deploying the code:
#### WHL method
Package the code into a whl file that contains all the code and python dependencies required. Use the following 
command to package it:
```bash
python3 -m pip build
```
> The python version and OS used to package the code must be the same of the nodes of the spark cluster.   
> The python library build must be installed beforehand. To install it use: `pip install build --upgrade`

Then, just install this package with all its dependencies into **every node of the cluster** with:
```bash
pip install multimno*.whl
```
> Internet connection is required to download all needed dependencies from the pypi repository

#### ZIP method
Zip all code under `multimno` directory into a single file. Then send it to the Spark server through the 
spark-submit configuration parameter: `--py-files=multimno.zip`. When using this method, the cluster must have the 
python dependencies installed beforehand.

### Install Dependencies
Multimno software is a pyspark application that needs both java and python depencies intalled to run.

#### Java Dependencies
The application uses the Apache Sedona engine to perform spatial calculations. In order to install this engine, 
the jar files must be downloaded to the cluster. These files can be downloaded at execution time through the maven repository, specifying 
them in the spark configuration, or they can be downloaded manually into the `$SPARK_HOME/jars` dir of every node in the cluster.

Reference - Sedona installation: https://sedona.apache.org/1.6.1/setup/cluster/

#### Python Dependencies

**A requirement of a pyspark application is that the python version must be alligned for all the cluster.**

The software needs a set of python dependencies to be installed in every node of the cluster. These dependencies will be 
installed automatically when using the [WHL method](setup_guide.md#whl-method). If you are using the [ZIP method](setup_guide.md#zip-method) 
you will need to install them manually into every node of the cluster through the requirements file. Install the dependencies of the 
`pyproject.toml` file into every node of the cluster.

## Local Mode
There are two ways of setting up a system for executing the source code in local mode:  
  1) Building the docker image provided. (Recommended)  
  2) Installing and setting up all required system libraries.  

### Docker setup

A Dockerfile is provided to build a docker image with all necessary dependencies for the code execution.

#### Installing docker software

To use the docker image it is necessary to have the docker engine installed. Please follow the official docker 
guide to set it up in your system:
-  Official guide: [Click here](https://docs.docker.com/engine/install/)

#### Docker image creation

Execute the following command:
```bash
docker build -t multimno:1.0-prod --target=multimno-prod .
```

#### Docker container creation

**Run an example pipeline within a container**
```bash
docker run --rm --name=multimno-container -v "${PWD}/sample_data:/opt/data" -v "${PWD}/pipe_configs:/opt/app/pipe_configs" multimno:1.0-prod pipe_configs/pipelines/pipeline.json
```

**Run a container in interactive mode**
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

## Software setup

The software is designed to be executed on a Linux operating system. **It is recommended to use Ubuntu 22.04 LTS.** However, the following environments are also supported:

- macOS: Version 12.6 or later.
- Windows 11: Using Windows Subsystem for Linux 2 (WSL2) with Ubuntu 22.04 as the configured distribution.

Please ensure that your system meets these requirements to guarantee optimal performance and compatibility.

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
pip install --upgrade pip uv
uv pip install -r pyproject.toml
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