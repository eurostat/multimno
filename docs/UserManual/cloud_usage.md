---
title: Cloud Deployment & Execution
weight: 4
---

# Cloud Deployment & Execution

## Introduction
In production environments, Multimno software will typically be deployed within a cloud infrastructure hosted on the premises of a Mobile Network Operator (MNO). This document outlines the standard workflow for provisioning the necessary cloud infrastructure, leveraging cloud-managed services for Big Data processing.

The document begins with a high-level overview of cloud-based deployments, followed by detailed, practical examples of implementation on AWS and GCP.

## Multimno Cloud Usage Requirements

### Overview
Multimno requires two primary components to operate effectively in a cloud environment:

- **Centralized Storage**  
    - **Description**: Centralized storage is a cloud-based storage solution that allows for the storage and retrieval of large amounts of data. It provides a scalable, durable, and highly available storage infrastructure. Centralized storage solutions typically support various data formats and access protocols, making them suitable for diverse data storage needs.
    - **Requirements**:
        - Network connectivity: Ensure that the compute engine cluster has network access to the centralized storage.
        - Centralized storage connectivity: Properly configure access permissions and credentials to allow seamless interaction between the compute engine cluster and the centralized storage.
    - **Recommended**: Cloud blob storage solutions such as Amazon S3 or Google Cloud Storage (GCS).

- **Compute Engine Cluster**
    - **Description**: A compute engine cluster is a collection of virtual machines or instances that work together to process large datasets using distributed computing frameworks like Hadoop, Spark, and others. These clusters could be managed by cloud providers, offering ease of setup, scaling, and maintenance.
    - **Requirements**:
        - Network connectivity: Ensure that the compute engine cluster can communicate with other necessary services, including centralized storage and other relevant cloud services if needed(Google Big Query, Amazon Redshift).
        - Centralized storage connectivity: The compute engine cluster must be able to read from and write to the centralized storage.
        - Hadoop ecosystem installed: The cluster should have the necessary Hadoop ecosystem components installed and configured, such as HDFS, YARN, Spark, and other relevant tools.
    - **Recommended**: Cloud-managed Hadoop ecosystem services such as Amazon EMR or Google Cloud Dataproc.  

### Additional Environments
Multimno is versatile and can be deployed in various Hadoop ecosystem environments, including but not limited to:  
- Kubernetes clusters  
- Serverless services  
- On-premise setups  

However, this documentation will focus exclusively on the recommended cloud-managed Hadoop services, such as Amazon EMR and Google Cloud Dataproc. Deployments in Kubernetes clusters, serverless services, and on-premise setups will not be covered in this document.


## Cloud Deployment

Multimno cloud deployment is based on the following steps:

1) Compile code and dependencies  
2) Configuration setup  
3) Input data setup  
4) Bootstrapping  
5) Execution  

### 1. Compile Code and Dependencies

To execute Multimno, the code and its dependencies need to be deployed to the cluster.

- **Compile Code to a `.whl` File**:  
    - Use the `./deployment/generate_deployment_package.sh` script to compile the code.
    - This script will generate a `.whl` file containing all the code and dependencies.
    - To execute the compilation, run:
        ```bash
        ./deployment/generate_deployment_package.sh
        ```
    - The compiled code in `.whl` format and the application entry points will be located in the newly created `upload` directory.

- **Upload `.whl` File to Cloud Storage**:
    - Upload the generated `.whl` file to the cloud storage (e.g., Amazon S3 or Google Cloud Storage).

- **Handle Dependencies Without Internet Connection**:
    - If the cluster does not have internet access, manually download dependencies and upload them to the cloud storage.
    - This includes Apache Sedona jars and Python dependencies.
    - Use the `./deployment/generate_deployment_package.sh` script with the `-j` (sedona jars) and `-d` (python dependencies) flags to download these files along with the compilation:
        ```bash
        ./deployment/generate_deployment_package.sh -j -d
        ```
    - The compiled code and jars will be located in the newly created `upload` directory. Upload both of them to the cloud storage.


### 2. Configuration Setup

Prepare the configuration files for the desired execution environment. Usually, you will need to change the values at the `[General]` 
section (country of study properties) and the value `home_dir` at the `[Paths]` section. 

Then for a concrete execution, edit the study dates of each of the components that will be launched.

Remember to read the [configuration guide](./configuration/index.md) before changing any settings to ensure proper configuration and avoid potential issues.

### 3. Input data setup

Follow the guidelines on the [execution guide](./execution.md#1-setting-input-data) for setting the input data and upload
it to the cloud storage.

### 4. Bootstrapping

The bootstrapping process involves setting up the necessary environment on the compute engine cluster to ensure that Multimno can execute correctly. This includes installing the code, configuring dependencies, and setting up the runtime environment. The following steps outline the detailed bootstrapping procedure:

#### Prerequisites

- Ensure that all necessary files, including the compiled `.whl` file and any additional dependencies, are uploaded to the cloud storage.

#### Steps

1. **Generate a Virtual Environment (venv)**:
    - Create a virtual environment to isolate the Multimno dependencies from the system-wide packages.
    - Execute the following command to create a virtual environment:
        ```bash
        python3 -m venv /path/to/venv --copies
        ```
    - Activate the virtual environment:
        ```bash
        source /path/to/venv/bin/activate
        ```

2. **Install the Code and Dependencies in the Virtual Environment**:
    - Install the compiled `.whl` file and its dependencies within the virtual environment.
    - If the cluster has internet access, use the following command to install the `.whl` file and dependencies:
        ```bash
        pip install /path/to/compiled_package.whl
        ```
    - If the cluster does not have internet access, manually install the dependencies:
        - Download the required dependencies and upload them to the cloud storage.
        - Use the following command to install the dependencies from the local files:
            ```bash
            pip install /path/to/additional_dependencies_dir/*
            ```

3. **Copy Apache Sedona JAR Files to `$SPARK_HOME/jars`**:
    - If the cluster does not have internet access, manually copy the Apache Sedona JAR files to the `$SPARK_HOME/jars` directory.
    - Ensure that the JAR files are uploaded to the cloud storage.
    - Use the following command to copy the JAR files to the appropriate directory:
        ```bash
        cp /path/to/sedona_jars/*.jar $SPARK_HOME/jars/
        ```

### 5. Execution

#### Software Execution in the Cloud

The execution phase involves running the Multimno pipeline, which is composed of multiple isolated components. These components are orchestrated by a Python script named `orchestrator_multimno.py`. This script is responsible for executing `spark-submit` commands for each component based on the configuration specified in a `pipeline.json` file. In a cloud environment, this script is initiated from the master node and performs `spark-submit` commands with the `master=yarn` setting, which is typically the default configuration in cloud-managed Hadoop ecosystems.

#### Steps for Execution

1. **Establish the Virtual Environment for Spark-Submit**:
    - Utilize the virtual environment created during the bootstrapping process.
    - Configure the `spark-submit` commands to use this virtual environment by setting the appropriate flags in the `pipeline.json` file.
    - The following `spark-submit` arguments should be included in the `pipeline.json` to ensure the correct Python environment is used:
        ```json
        {
            "spark_submit_args": [
                "--conf=spark.pyspark.python=/opt/venv/multimno/bin/python3",
                "--conf=spark.yarn.appMasterEnv.PYSPARK_PYTHON=/opt/venv/multimno/bin/python3",
                "--conf=spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=/opt/venv/multimno/bin/python3",
                "--conf=spark.executorEnv.PYSPARK_PYTHON=/opt/venv/multimno/bin/python3"
            ]
        }
        ```

2. **Execute the Orchestrator Script**:
    - Launch the `orchestrator_multimno.py` script using the virtual environment to ensure all dependencies are correctly resolved.
    - Execute the script with the following command:
        ```bash
        /opt/venv/multimno/bin/python3 /opt/workdir/multimno/orchestrator_multimno.py configurations/pipeline.json
        ```

## Cloud Examples

In this section, we provide detailed examples of bootstrapping and execution for the recommended cloud-managed Hadoop services. These examples are intended to guide users through the process of setting up and running the Multimno pipeline on Amazon EMR and Google Dataproc.

### Amazon EMR

The directory `deployment/aws_emr_example` contains comprehensive examples of bootstrap and execution scripts specifically tailored for AWS EMR version 7.2.0.

#### Bootstrap

The file `deployment/aws_emr_example/bootstrap_emr_720.sh` serves as an exemplary bootstrap script for environments with internet connectivity. This script performs the following actions:

1. **Download the Code**:
    - The script retrieves the necessary code from the specified cloud storage or repository.

2. **Setup Sedona JARs**:
    - It configures the Apache Sedona JAR files required for spatial data processing by copying them to the appropriate directory.

3. **Python Environment Setup**:
    - The script creates a Python virtual environment to isolate the dependencies.
    - It installs the Multimno code along with all required dependencies within this virtual environment.

This bootstrap script ensures that the AWS EMR cluster is properly configured with all necessary components to run the Multimno pipeline.

#### Execution

The script `deployment/aws_emr_example/launch.sh` provides an example of how to execute the Multimno pipeline using the `orchestrator_multimno.py` script. In the context of AWS EMR, this script can be executed as an EMR Step. The following points outline the execution process:

1. **EMR Step Configuration**:
    - Since the execution involves running an orchestrator script rather than a direct `spark-submit` command, the step must be configured to launch a bash command.
    - This is achieved using the `command-runner.jar`, which allows the execution of arbitrary shell commands on the EMR cluster.

2. **Launching the Script**:
    - The `command-runner.jar` is used to execute the `launch.sh` script, which in turn runs the `orchestrator_multimno.py` script with the specified configuration file (`pipeline.json`).
    - Example:
        ```bash
        aws emr add-steps --cluster-id $cluster_id --steps Type=CUSTOM_JAR,Name="RunMultimnoOrchestrator",ActionOnFailure=CONTINUE,Jar="command-runner.jar",Args=["bash","/opt/workdir/code/launch.sh", "$pipeline_path"]
        ```


### Google Dataproc

The directory `deployment/gcp_dataproc_example` contains comprehensive examples of bootstrap and execution scripts specifically tailored for Google Dataproc.

#### Bootstrap

The file `deployment/gcp_dataproc_example/bootstrap_dataproc_22.sh` serves as an exemplary bootstrap script for environments with no internet connectivity. This script performs the following actions:

1. **Download the Code**:
    - The script retrieves the necessary code from the specified cloud storage or repository.

2. **Setup Sedona JARs**:
    - It configures the Apache Sedona JAR files required for spatial data processing by copying them to the appropriate directory.

3. **Python Environment Setup**:
    - The script creates a Python virtual environment to isolate the dependencies.
    - It installs the Multimno code along with all required dependencies within this virtual environment. It assumes all dependencies have been setup at cloud storage.

This bootstrap script ensures that the Google Dataproc cluster is properly configured with all necessary components to run the Multimno pipeline.

#### Execution

The script `deployment/gcp_dataproc_example/launch.sh` provides an example of how to execute the Multimno pipeline using the `orchestrator_multimno.py` script. In the context of Google Dataproc, this script can be executed as a Dataproc Job. The following points outline the execution process:

1. **Dataproc Job Configuration**:
    - Since the execution involves running an orchestrator script rather than a direct `spark-submit` command, the job must be configured to launch a bash command.
    - This is achieved using the `Pig` job which can launch a custom command.

2. **Launching the Script**:
    - The `gcloud` command is used to submit the `launch.sh` script as a Dataproc Job, which in turn runs the `orchestrator_multimno.py` script with the specified configuration file (`pipeline.json`).
    - Example:
        ```bash
        gcloud dataproc jobs submit pig  \
            --region=$region \
            --cluster=$cluster \
            --async \
            --jars="gs://$bucket/eurostat/code/launch.sh" \
            -e="sh /opt/workdir/code/launch.sh $pipeline_path"
        ```
