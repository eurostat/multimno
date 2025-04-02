#!/bin/bash

set -e # Set exit on command failure

work_dir=/opt/workdir
venv_dir=/opt/venv/multimno
bucket=$1 
mulitmno_version=$2

sudo mkdir -p $work_dir/code

# Download code + entrypoints
sudo aws s3 cp s3://$bucket/code $work_dir/code --recursive

# Setup Jars 
gsutil -m cp gs://$bucket/multimno/env/jars/* /usr/lib/spark/jars/

# ---------- Setup Venv ----------
mkdir -p /opt/venv
sudo python3.11 -m venv $venv_dir --copies

# Install software + dependencies
sudo $venv_dir/bin/python3 -m pip install $work_dir/code/multimno-$mulitmno_version-py3-none-any.whl

# ------------------------

# Download configurations
sudo mkdir -p $work_dir/configurations
sudo aws s3 cp s3://$bucket/configurations $work_dir/configurations --recursive


# Set permissions
sudo chown -R $USER:$USER $work_dir
sudo chown -R $USER:$USER $venv_dir
sudo chmod -R 755 $work_dir

