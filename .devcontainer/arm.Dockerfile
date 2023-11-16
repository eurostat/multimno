ARG PYTHON_VERSION

FROM python:${PYTHON_VERSION}-bullseye

ARG JDK_VERSION
# ---------- INSTALL Java + GDAL ----------
# Needed for Pyspark
RUN apt update && apt install -y \
    openjdk-${JDK_VERSION}-jdk \
    libgdal-dev 

# Set JAVA_HOME environment variable
# TODO: Change JAVA_HOME path for ARM architecture
ENV JAVA_HOME="/usr/lib/jvm/java-${JDK_VERSION}-openjdk-amd64"
ENV PATH="${PATH}:${JAVA_HOME}/bin"

# ---------- INSTALL poetry / pip-tools ----------
RUN pip3 install poetry pip-tools

# Install requirements
ARG install_dir=/tmp/install
ARG SPARK_VERSION

# Install pyspark
RUN pip3 install pyspark==${SPARK_VERSION}

# Standard requirements
COPY resources/requirements/requirements.in ${install_dir}/requirements/requirements.in
RUN pip-compile ${install_dir}/requirements/requirements.in && pip install -r ${install_dir}/requirements/requirements.txt
# Dev requirements
COPY resources/requirements/dev_requirements.txt ${install_dir}/requirements/dev_requirements.txt
RUN pip install -r ${install_dir}/requirements/dev_requirements.txt

# # Add jupyterlab alias
RUN echo "alias jl='jupyter lab --ip=0.0.0.0 --port=8888 --no-browser  \
    --allow-root --NotebookApp.base_url=${JUPYTER_BASE_URL} --NotebookApp.token='" >> ~/.bashrc

# ----------- CLEANUP -----------
RUN rm -r ${install_dir}
RUN rm -rf /var/lib/apt/lists/*

# ----------- RUNTIME -----------
EXPOSE 8888

CMD ["bash"]