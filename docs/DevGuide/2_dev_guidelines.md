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
PYTHON_VERSION=3.11 # Python version.
JDK_VERSION=17 # Java version.
SPARK_VERSION=3.4.1 # Spark/Pyspark version.
SCALA_VERSION=2.12 # Spark dependency.
SEDONA_VERSION=1.5.1 # Sedona
GEOTOOLS_WRAPPER_VERSION=28.2 # Sedona dependency

# ------------------- Docker run parameters -------------------
CONTAINER_NAME=multimno_dev_container # Container name.
DATA_DIR=../sample_data # Path of the host machine to the data to be used within the container.
SPARK_LOGS_DIR=../sample_data/logs # Path of the host machine to where the spark logs will be stored.
JL_PORT=8888 # Port of the host machine to deploy a jupyterlab.
JL_CPU=4 # CPU cores of the container.
JL_MEM=16g # RAM of the container.
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
spark-submit multimno/main.py <component_id> <path_to_general_config> <path_to_component_config> 
```

Example:  
  
```bash
spark-submit multimno/main.py SyntheticEvents pipe_configs/configurations/general_config.ini pipe_configs/configurations/synthetic_events/synth_config.ini 
```

### Launching a pipeline
In a terminal execute the command:  

```bash
python multimno/orchestrator.py <pipeline_json_path>
```

Example:  

```
python multimno/orchestrator.py pipe_configs/pipelines/pipeline.json 
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

### Code Linting

The python code generated shall be formatted with black. For formatting all source code execute the
following command:

```bash
black -l 120 multimno tests/test_code/
```

### Clean jupyter notebooks

```bash
find ./notebooks/ -type f -name \*.ipynb | xargs jupyter nbconvert --clear-output --inplace
```

### Code Security Scan

```bash
bandit -r multimno
```


## 🧪 Testing

### Launch Tests

#### Manual

```bash
pytest tests/test_code/multimno
```

#### VsCode

1) Open the test view at the left panel. 
2) Launch tests.


### Generate test & coverage reports

Launch the command

```bash
pytest --cov-config=tests/.coveragerc \
    --cov-report="html:docs/autodoc/coverage" \
    --cov=multimno --html=docs/autodoc/test_report.md \
    --self-contained-html tests/test_code/multimno
```

Test reports will be stored in the dir: `docs/autodoc`


### See coverage in IDE (VsCode extension)
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




