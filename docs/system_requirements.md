# System Requirements

Multimno is a python library which requires the installation of additional system & python libraries.In this section the requirements for executing this software are defined. 

In the case of using the docker image provided,
the system only needs to comply with the [Hardware Requirements](./system_requirements.md#hardware-requirements) and [Docker requirements](./system_requirements.md#docker-requirements) as the docker image will have all the [software requirements](./system_requirements.md#software-requirements) already installed.

## Hardware requirements

### Minimum requirements

The hardware specification needed will vary depending on the input data volumetry. However, we recommend this settings as **minimum requirements** for a single node cluster:

- **Cores:** 4
- **RAM:** 16 Gb
- **Disk:** 32 Gb of free space
- **OS:** 
    - **Ubuntu 22.04 (Recommended)**
    - Mac 12.6
    - Windows 11 + WSL2 with Ubuntu 22.04 
  
## Software Requirements

### OS Libraries

| Library      | Version |
| ------------ | ------- |
| Python       | >= 3.9  |
| Java JDK     | 17.0.9  |
| Apache Spark | 3.5.1   |
| GDAL         | 3.6.2   |

### Spark Libraries (jars)

| Library          | Version |
| ---------------- | ------- |
| Apache Sedona    | 1.6.0   |
| Geotools wrapper | 28.2    |

### Python Libraries

| Library       | Version      |
| ------------- | ------------ |
| numpy         | >=1.24,<1.27 |
| pandas        | >=2.0,<2.3   |
| pyarrow       | >=17.0       |
| requests      | 2.31.0       |
| ujson         | 5.9          |
| toml          | 0.10         |
| apache-sedona | 1.6.0        |
| geopandas     | 0.11.1       |
| shapely       | 1.8.4        |
| pyspark       | 3.5.1        |
| py4j          | >=0.10.9.7   |

## Docker requirements
In the case of using the docker image provided for single node execution the following requirements must be fulfilled:
  - **Docker-engine:** >=25.X
  - **Docker-compose:** >=2.24.X
  - Internet connection to Ubuntu/Spark/Docker official repositories for building the docker image