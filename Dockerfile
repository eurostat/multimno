##########################################################################
# MULTIMNO - BASE
##########################################################################

FROM ubuntu:22.04 as multimno-base

# --------- Set tzdata ----------
# Set the timezone
ENV TZ=Europe/Madrid
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get update && apt-get install -y tzdata


# --------- INSTALL Python --------
RUN apt update && \
  apt install -y software-properties-common curl

RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && apt install -y python3.11-dev

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Install pip
RUN curl https://bootstrap.pypa.io/get-pip.py | python

ARG JDK_VERSION
# ---------- INSTALL System Libraries ----------
# Needed for Pyspark
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  sudo \
  openjdk-17-jdk \
  build-essential \
  software-properties-common \
  openssh-client openssh-server \
  gdal-bin \
  libgdal-dev \
  ssh \
  wget

# ---------- SPARK ----------
# Setup the directories for Spark/Hadoop installation
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}

# Create spark folder
RUN mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

ARG SPARK_VERSION=3.5.1
# Download and install Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
  && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
  && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_VERSION=${SPARK_VERSION}

# ---------- SEDONA ----------
# Args
ARG SCALA_VERSION=2.12
ARG SEDONA_VERSION=1.6.0
ARG GEOTOOLS_WRAPPER_VERSION=28.2

ENV SCALA_VERSION=${SCALA_VERSION}
ENV SEDONA_VERSION=${SEDONA_VERSION}
ENV GEOTOOLS_WRAPPER_VERSION=${GEOTOOLS_WRAPPER_VERSION}

# Install sedona jars
COPY resources/scripts/install_sedona_jars.sh ${install_dir}/scripts/install_sedona_jars.sh
RUN ${install_dir}/scripts/install_sedona_jars.sh ${SPARK_VERSION} ${SCALA_VERSION} ${SEDONA_VERSION} ${GEOTOOLS_WRAPPER_VERSION} 

# ---------- PYTHON DEPENDENCIES ----------
# Install requirements
ARG install_dir=/tmp/install

# Upgrade pip to latest version
RUN pip install --upgrade pip

# Install python3-dev for libraries
RUN apt-get update && apt-get install -y python3-dev

# Standard requirements
RUN pip install uv
COPY pyproject.toml ${install_dir}/requirements/pyproject.toml
RUN uv pip install -r ${install_dir}/requirements/pyproject.toml --system --extra=spark

# Set Path environment variable
ENV PATH="${PATH}:$SPARK_HOME/bin:$SPARK_HOME/sbin"

ENV PYTHONPATH=${SPARK_HOME}/python:/opt/app
WORKDIR /opt/app
EXPOSE 4040

# ----------- CLEANUP -----------
RUN rm -r ${install_dir}
RUN rm -rf /var/lib/apt/lists/*


##########################################################################
# MULTIMNO - DEV
##########################################################################
FROM multimno-base as multimno-dev


# Install git
RUN apt update && apt install -y git 

# Install requirements
ARG install_dir=/tmp/install

# Dev requirements
COPY pyproject.toml ${install_dir}/requirements/pyproject.toml
RUN uv pip install -r ${install_dir}/requirements/pyproject.toml --extra=dev --system

# # Add jupyterlab alias
RUN echo "alias jl='jupyter lab --ip=0.0.0.0 --port=8888 --no-browser  \
  --allow-root --NotebookApp.base_url=${JUPYTER_BASE_URL} --NotebookApp.token='" >> ~/.bashrc

# ----------- CLEANUP -----------
RUN rm -r ${install_dir}
RUN rm -rf /var/lib/apt/lists/*

# Install pip & build for python3.10 environment
RUN curl https://bootstrap.pypa.io/get-pip.py | python3.10
RUN python3.10 -m pip install --upgrade build

# ----------- RUNTIME -----------
# Copy the default configurations into $SPARK_HOME/conf
COPY resources/conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"
COPY resources/conf/log4j2.properties "$SPARK_HOME/conf/log4j2.properties"

EXPOSE 8888

CMD ["bash"]

##########################################################################
# MULTIMNO - Production
##########################################################################
FROM multimno-base as multimno-prod

RUN mkdir -p /tmp/spark-events

COPY multimno /opt/app/multimno
COPY pipe_configs /opt/app/pipe_configs
COPY orchestrator_multimno.py /opt/app/orchestrator_multimno.py

# ENTRYPOINT ["python", "/opt/app/orchestrator_multimno.py"]