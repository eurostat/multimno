# MultiMNO <!-- omit from toc -->

This repository contains code that processes MNO Data to generate population and mobility insights indicators using the Spark framework. 


- [📒 Description](#-description)
- [🗃️ Repository Structure](#️-repository-structure)
- [📄 Documentation](#-documentation)
  - [📓 User Manual](#-user-manual)
  - [🤝 Contribute](#-contribute)
  - [🖥️ Developement Guidelines](#️-developement-guidelines)
- [📜 Licensing](#-licensing)
  - [Licensing Files](#licensing-files)
- [🅿️ Pipeline](#️-pipeline)
- [🛠️ Mandatory Requirements](#️-mandatory-requirements)
- [📦 Synthetic data](#-synthetic-data)
- [🏁 Quickstart](#-quickstart)
  - [Setup](#setup)
  - [Execution](#execution)


# 📒 Description

This repository contains a python application that uses the PySpark library to process Big Data pipelines of MNO Data
and generate multiple stadistical products related to mobility and sociodemographic analysis.

The code stored in this repository is aimed to be executed in a PySpark compatible cluster and to be deployed in 
cloud environments like AWS, GCP or Azure. Nevertheless, the code can be launched in local environments using a single 
node Spark configuration once all the required libraries have been correctly set. 

For an easy deployment in local environments, configuration for creating a docker container 
with all the setup done is provided in this repository.

# 🗃️ Repository Structure

The repository contains the following directories:

| Directory         | Type                                         | Description                                                                                                  |
| ----------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **.devcontainer** | $${\color{#ac8e03}Development}$$             | Directory with config files for setting up a dev-environment using [Dev-Containers.](https://containers.dev) |
| **.vscode**       | $${\color{#ac8e03}Development}$$             | Directory containing config files for developers using VsCode.                                               |
| **docs**          | $${\color{#7030a0}Technical-Documentation}$$ | Documentation source files that will be used for the documentation site. Mainly markdown files.              |
| **license**          | $${\color{#7030a0}Technical-Documentation}$$ | Directory containing the Software Bill of Materials (SBOM) and associated licensing documentation for software dependencies.     |
| **multimno**      | $${\color{#ff0000}Open-source-software}$$    | Main directory of the repository. It contains the Python source code of the application.                     |
| **pipe_configs**  | $${\color{#0070c0}Data}$$                    | Directory containing examples of configuration files for the execution of the pipeline.                      |
| **sample_data**   | $${\color{#0070c0}Data}$$                    | Directory containing Synthetic MNO-Data to be used to test the software.                                     |
| **resources**     | $${\color{#ff0000}Open-source-software}$$    | Directory containing requirements files and development related configuration and script files.              |
| **tests**         | $${\color{#00b050}Testing}$$                 | Directory containing test code and test files for the testing execution.                                     |

---

# 📄 Documentation

The multimno documentation is divided into two main documents.

- [Technical documentation](https://cros.ec.europa.eu/group/6/files/2490/download): PDF file deatiling the software requirements, design and data objects.
- [User/Developer manual](https://eurostat.github.io/multimno/latest/): Webpage containing the user and developer manuals, including the contribute guide.

## 📓 User Manual

A user manual is provided composed of three sections:
* [Configuration](docs/UserManual/configuration/index.md): Section containing the explanation of all the configuration files used by the software. 
* [Setup Guide](docs/UserManual/setup_guide.md): How to prepare the system for the software execution.
* [Execution Guide](docs/UserManual/execution.md): How to execute the software.   

## 🤝 Contribute

Please follow the [contribute guide](docs/DevGuide/1_contribute.md) to see the rules and guidelines on 
how to contribute to the multimno repository.

## 🖥️ Developement Guidelines

Please follow the [development guidelines](docs/DevGuide/2_dev_guidelines.md) to setup a dev-environment and 
see the recommended best practices for development, testing and documentation.

# 📜 Licensing

Multimno software is licensed under the European Union Public License (EUPL) 1.2 as declared by its [LICENSE](./LICENSE) file. To ensure transparency in its dependencies, a Software Bill of Materials (SBOM) is provided at [license/sbom.json](license/sbom.json). This SBOM was generated on March 18, 2025, using [CycloneDX](https://cyclonedx.org).

## Licensing Files
The SBOM is generated using CycloneDX, and an accompanying Python script is provided to facilitate further analysis. This script processes the SBOM file and produces the `licenses.csv` file and the `unique_licenses.txt`. All license related files are stored at the `licenses/` directory, containing:

- `licenses.csv` – A concise CSV file containing the list of dependencies, including their versions and associated licenses.
- `sbom.json` – A comprehensive SBOM detailing all dependencies and their respective licenses.
- `unique_licenses.txt` – A file enumerating the distinct licenses used within the software.

As of March 18, 2025, the licensing data has been included in the repository and validated against the [EUPL matrix of compatible open-source licenses](https://interoperable-europe.ec.europa.eu/collection/eupl/matrix-eupl-compatible-open-source-licences).

# 🅿️ Pipeline

The pipeline of Big Data processing performed by the software can be found at 
the following document: [MultiMNO Pipeline](docs/pipeline.md)

# 🛠️ Mandatory Requirements

Please verify that your system fullfils the [System Requirements.](docs/system_requirements.md#system-libraries) in order to assert that your system can execute the code. 

# 📦 Synthetic data

MNO synthetic data is given in the repository under the `sample_data/lakehouse/bronze` directory. This data 
has been generated synthetically and contains the following specs:  

- 🌍 **Spatial scope:** All data has been generated in a bounding box that covers the metropolitan area of Madrid.
 The bounding box parameters are as follows:
  - latitude_min = 40.352
  - latitude_max = 40.486
  - longitude_min = -3.751
  - longitude_max = -3.579


- 📆 **Temporal scope :** Data has been generated for 9 days, from 2023-01-01 to 2023-01-09 both included.
- 🚶‍♂️**Users:** 100 different users.
- 📡**Network:** 500 different cells.
  
# 🏁 Quickstart

## Setup
Use the following commands for a fast setup of an execution environment using docker. 

> Please check the [Setup Guide](docs/UserManual/setup_guide.md) for a more indepth detail
>  of the system setup to execute the code.

Build docker image
```bash
docker build -t multimno:1.0-prod --target=multimno-prod .
```

## Execution

Run an example pipeline within a container:
```bash
docker run --rm --name=multimno-container -v "${PWD}/sample_data:/opt/data" -v "${PWD}/pipe_configs:/opt/app/pipe_configs" multimno:1.0-prod pipe_configs/pipelines/pipeline.json
```

This command will:
- Create a docker container.
- mount the `sample_data` directory in `/opt/data` within the container.
- mount the `pipe_configs` directory in `/opt/app/pipe_configs` within the container.
- Execute a pipeline stored in `/opt/app/pipe_configs/pipelines/pipeline.json` within the container. This is the same file
as the one in the repository.
- Delete the container once the execution finishes.

> NOTE: It is necessary to adjusts paths in the pipeline.json and in the general_configuration.ini 
> file if the destination paths are altered.
