##########################################################################
# SPARK - BASE
##########################################################################

ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION} as multimno-spark-base

ARG JDK_VERSION
# ---------- INSTALL System Libraries ----------
# Needed for Pyspark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      openjdk-${JDK_VERSION}-jdk \
      build-essential \
      software-properties-common \
      openssh-client openssh-server \
      gdal-bin \
      libgdal-dev \
      ssh

# ---------- SPARK ----------
# Setup the directories for Spark/Hadoop installation
ENV SPARK_HOME=${SPARK_HOME:-"/opt/spark"}
ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/hadoop"}

# Create spark folder
RUN mkdir -p ${HADOOP_HOME} && mkdir -p ${SPARK_HOME}
WORKDIR ${SPARK_HOME}

ARG SPARK_VERSION
# Download and install Spark
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
  && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
  && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV PYTHONPATH=${SPARK_HOME}/python
# RUN pip install pyspark==${SPARK_VERSION}

# ---------- SEDONA ----------
# Args
ARG SEDONA_VERSION
ARG GEOTOOLS_WRAPPER

# Install sedona jars
COPY resources/scripts/install_sedona_jars.sh ${install_dir}/scripts/install_sedona_jars.sh
RUN ${install_dir}/scripts/install_sedona_jars.sh ${SEDONA_VERSION} ${GEOTOOLS_WRAPPER} ${SPARK_VERSION} $PYTHON_VERSION

# ---------- PYTHON DEPENDENCIES ----------
# ---------- INSTALL poetry / pip-tools ----------
RUN pip3 install poetry pip-tools

# Install requirements
ARG install_dir=/tmp/install

# Standard requirements
COPY resources/requirements/requirements.in ${install_dir}/requirements/requirements.in
RUN pip-compile ${install_dir}/requirements/requirements.in 
RUN pip install -r ${install_dir}/requirements/requirements.txt
# Dev requirements
COPY resources/requirements/dev_requirements.in ${install_dir}/requirements/dev_requirements.in
RUN pip-compile ${install_dir}/requirements/dev_requirements.in
RUN pip install -r ${install_dir}/requirements/dev_requirements.txt

# # Add jupyterlab alias
RUN echo "alias jl='jupyter lab --ip=0.0.0.0 --port=8888 --no-browser  \
    --allow-root --NotebookApp.base_url=${JUPYTER_BASE_URL} --NotebookApp.token='" >> ~/.bashrc


# ----------- CLEANUP -----------
RUN rm -r ${install_dir}
RUN rm -rf /var/lib/apt/lists/*

# ----------- RUNTIME -----------
# Copy the default configurations into $SPARK_HOME/conf
COPY resources/conf/spark-defaults.conf "$SPARK_HOME/conf/spark-defaults.conf"

EXPOSE 8888

CMD ["bash"]