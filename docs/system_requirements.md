# System Requirements

Multimno is a python library which requires the installation of additional system & python libraries.

In this section the requirements for executing this software are defined. In the case of using docker for execution,
the system only needs to comply with the [Host](./system_requirements.md#system-requirements) and [Docker requirements](./system_requirements.md#docker-requirements) as the docker image will have all the [software requirements](./system_requirements.md#software-requirements).

## Host requirements

- **Cores:** 4
- **RAM:** 16 Gb
- **Disk:** 32 Gb of free space
- **OS:** 
    - **Ubuntu 22.04 (Recommended)**
    - Mac 12.6
    - Windows 11 + WSL2 with Ubuntu 22.04 

## Docker requirements
In the case of using the docker image provided the following requirements must be fulfilled:

  - **Docker-engine:** 25.0.X
  - **Docker-compose:** 2.24.X
  - Internet connection to Ubuntu/Spark/Docker official repositories for building the docker image
  
## Software Requirements

In the case of setting up a system for launching the software, the following dependencies have to be installed.

### OS Libraries

| Library      | Version  |
| ------------ | -------- |
| Python       | > 3.10.8 |
| Java JDK     | 17.0.9   |
| Apache Spark | 3.5.1    |
| GDAL         | 3.6.2    |

### Spark Libraries

| Library          | Version |
| ---------------- | ------- |
| Apache Sedona    | 1.5.1   |
| Geotools wrapper | 28.2    |

### Python Libraries

| Library   | Version  |
| --------- | -------- |
| numpy     | 1.26.2   |
| pandas    | 2.1.4    |
| geopandas | 0.11.1   |
| shapely   | 1.8.4    |
| pyarrow   | 14.0.1   |
| requests  | 2.31.0   |
| py4j      | 0.10.9.7 |
| pydeck    | 0.8.0    |

