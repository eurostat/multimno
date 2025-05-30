# Developer Guidelines

The repository contains a devcontainer configuration compatible with VsCode. This configuration will create a docker container with all the necessary libraries and configurations to develop and execute the source code. 

## 🛠️ Development Environment Setup 

This repository provides a docker dev-container with a system completely configured to execute the source code as well as useful libraries and tools for the development of the multimno software. 
Thanks to the dev-container, it is guaranteed that all developers are developing/executing code in the same environment.

The dev-container specification is all stored inside the `.devcontainer` directory.

### Configuring the docker container

Configuration parameters for building the docker image and for creating the docker container are specified in
the configuration file `.devcontainer/.env` file. This file
contains user specific container configuration (like the path in the host machine to the data). As this file will change 
for each developer it is ignored for the git version control and **must be created after cloning the repository**.

A `template file` is stored in this repostiory. You can use this
file as a baseline copying it to the `.devcontainer` directory.

```bash
cp resources/templates/dev_container_template.env .devcontainer/.env
```

Please edit the `.devcontainer/.env` file `Docker run parameters` section 
with your preferences:

```ini
# ------------------- Docker Build parameters -------------------
SPARK_VERSION=3.5.1 # Spark/Pyspark version.
SCALA_VERSION=2.12 # Spark dependency.
SEDONA_VERSION=1.6.1 # Sedona
GEOTOOLS_WRAPPER_VERSION=28.2 # Sedona dependency

# ------------------- Docker run parameters -------------------
CONTAINER_NAME=multimno_dev_container # Container name.
DATA_DIR=../sample_data # Path of the host machine to the data to be used within the container.
SPARK_LOGS_DIR=../sample_data/logs # Path of the host machine to where the spark logs will be stored.
JL_PORT=8888 # Port of the host machine to deploy a jupyterlab.
SUI_PORT=4040 # Port for the Spark UI.
SHS_PORT=18080 # Port for the Spark History Server.
CONTAINER_CPU=4 # CPU cores of the container.
CONTAINER_MEM=24g # RAM of the container.
```

### Starting the dev environment 

#### VsCode
Prerequisite: Dev-Container/Docker extension

In VsCode: **F1 -> Dev Containers: Rebuild and Reopen in container**

#### Manual

##### Docker image creation

Execute the following command:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env build
```

##### Docker container creation
Create a container and start a shell session in it with the commands:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env up -d
docker exec -it multimno_dev_container bash
```

### Stopping the dev environment

#### VsCode

Closing VsCode will automatically stop the devcontainer.

#### Manual
Exit the terminal with:

Ctrl+D or writing `exit`

### Deleting the dev environment

Delete the container created with:
```bash
docker compose -f .devcontainer/docker-compose.yml --env-file=.devcontainer/.env down
```

## 🐎 Execution

### Launching a single component
In a terminal execute the command:  
  
```bash
spark-submit multimno/main_multimno.py <component_id> <path_to_general_config> <path_to_component_config> 
```

Example:  
  
```bash
spark-submit multimno/main_multimno.py SyntheticEvents pipe_configs/configurations/general_config.ini pipe_configs/configurations/synthetic_events/synth_config.ini 
```

### Launching a pipeline
In a terminal execute the command:  

```bash
python multimno/orchestrator_multimno.py <pipeline_json_path>
```

Example:  

```
python multimno/orchestrator_multimno.py pipe_configs/pipelines/pipeline.json 
```

## 🔍 Monitoring/Debug

### Launching a spark history server
The history server will access SparkUI logs stored at the path ${SPARK_LOGS_DIR} defined in the `.devcontainer/.env` file.

Starting the history server
```bash
start-history-server.sh 
```
Accesing the history server
* Go to the address http://localhost:18080

## 🪶 Style

### Coding style

The code shall follow the standard **PEP 8** which is the coding style proposed for writing clean, readable, and maintainable Python code. 
It was created to promote consistency in Python code and make it easier for developers to collaborate on projects. 

[PEP 8 official guide](https://peps.python.org/pep-0008/)

### Docstring style
The docstrings written in the code shall follow the **Google Docstrings** style. Adhering  to a unique docstring style 
guarantees consistency within software development in a project. Google Docstrings are the most popular convention for 
docstrings which facilitates readability and collaboration in open-source projects. 

[Google Docstrings official guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)

## 🧼 Code cleaning

Developed code shall be formatted and jupyter notebooks shall be cleaned of outputs to guarantee consistency and reduce 
unnecessary differences between commits.

### Pre-Commit Git Hook
The repository includes a pre-commit Git hook located at `resources/hooks/pre-commit`. This hook is designed to be executed automatically before any commit to the repository. It performs code linting and Jupyter notebook cleaning as defined in the subsequent sections.

#### Automatic Setup
The pre-commit hook is automatically configured when launching the devcontainer through Visual Studio Code.

#### Manual Setup
To manually set up the pre-commit hook, execute the following command:

```bash
./resources/hooks/set_git_hooks.sh
```

This ensures that the pre-commit hook is properly installed and executable, thereby maintaining code quality and consistency across the repository.

### Code Linting

The python code generated shall be formatted with black. For formatting all source code execute the
following command:

```bash
black -l 120 multimno tests/test_code/
```

### Clean jupyter notebooks (if any)

If any jupyter notebooks have been added, it is important that outputs are cleaned to avoid adding unecessary information to the git history.

```bash
find ./ -type f -name \*.ipynb | xargs jupyter nbconvert --clear-output --inplace
```


### Code Security Scan
A static code analysis should be performed to identify common security issues in the codebase. The recommended tool for this purpose is `bandit`, which scans Python code for vulnerabilities and insecure coding practices.

```bash
bandit -r multimno
```


## 🧪 Testing

Tests can be executed manually using the command line or through the integrated testing tools in Visual Studio Code. This ensures that all components function as expected before code is merged. 

For each component, the testing framework generates both synthetic input data and the corresponding expected output data. The component is then executed using the synthetic data, and the output produced is compared against the expected results. Assertions are made to ensure that the generated data matches the expected data exactly. This approach guarantees that each component behaves as intended and provides a robust foundation for safely evolving and refactoring the codebase over time.


### Launch Tests

#### Manual

```bash
pytest tests/test_code/multimno
```

#### VsCode

1) Open the test view at the left panel. 
2) Launch tests.


### Generate test & coverage reports

Test and coverage reports can be generated to assess the quality and completeness of the test suite. Reports are stored in the designated documentation directory for review.


Launch the command

```bash
pytest --cov-config=tests/.coveragerc \
    --cov-report="html:docs/autodoc/coverage" \
    --cov=multimno --html=docs/autodoc/test_report.md \
    --self-contained-html tests/test_code/multimno
```

Test reports will be stored in the dir: `docs/autodoc`


### See coverage in IDE (VsCode extension)
Test coverage can be visualized directly within Visual Studio Code using the Coverage Gutters extension. This provides immediate feedback on which parts of the codebase are covered by tests.

1) Launch tests with coverage to generate the coverage report (xml)
```bash
pytest --cov-report="xml" --cov=multimno tests/test_code/multimno
```
1) Install the extension: Coverage Gutters
2) Right click and select Coverage Gutters: Watch

*Note: You can see the coverage percentage at the bottom bar*


## 📄 Code Documentation

### Documentation server Debug(Mkdocs)

A code documentation can be deployed using mkdocs backend. 

1) Create documentation (This will launch all tests)
```bash
./resources/scripts/generate_docs.sh
```
2) Launch doc server

```bash
mkdocs serve
```
and navigate to the address: http://127.0.0.1:8000


### Documentation deploy (mike)

Set `latest` as default version
```bash
mike set-default --push latest
```

Deploy a version of the documentation with:

```bash
mike deploy --push --update-aliases <version> latest
```

Example:

```bash
mike deploy --push --update-aliases 0.2 latest
```

# 📜 Licensing

## Generating License Data

Licensing data can be regenerated using the development container (devcontainer) and the scripts located in the `resources/scripts/license/` directory. These scripts include:

- `generate_license.sh` – A Bash script that invokes CycloneDX to generate the SBOM.
- `generate_concise_license_data.py` – A Python script that parses the SBOM and produces the concise licensing data. This script is automatically executed by `generate_license.sh`.

### Usage
To generate the licensing data, execute the following commands within the devcontainer terminal:

```bash
./resources/scripts/license/generate_license.sh
```

This process will update the licensing files accordingly.

