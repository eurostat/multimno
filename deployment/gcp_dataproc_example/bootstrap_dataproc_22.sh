#!/bin/bash

set -ex # Set exit on command failure

work_dir=/opt/workdir
venv_dir=/opt/venv/multimno

bucket=$(/usr/share/google/get_metadata_value attributes/multimno_bucket)
multimno_version=$(/usr/share/google/get_metadata_value attributes/multimno_version)

scenario_path="${bucket}/multimno"


# ------------ MAIN ------------

mkdir -p "$work_dir"

# Download code
gsutil -m cp -r "gs://$scenario_path/code/" "$work_dir"

# Setup Jars 
gsutil -m cp gs://$bucket/multimno/env/jars/* /usr/lib/spark/jars/

# ---------- Setup ENV ----------
mkdir -p /opt/venv
sudo python3.11 -m venv $venv_dir --copies

# Install dependencies
$venv_dir/bin/python3 -m pip install "$work_dir/code/deps/*" --no-deps

# Install software
$venv_dir/bin/python3 -m pip install "$work_dir/code/multimno-$multimno_version-py3-none-any.whl" --no-deps --force-reinstall

# ------------------------


# ---------- Setup Configuration ----------
# Download configurations
mkdir -p "$work_dir/configurations"
gsutil -m cp -r "gs://$scenario_path/configurations" "$work_dir"


# ---------- Setup Permissions ----------
# Set permissions
sudo chown -R $USER:$USER $work_dir
sudo chown -R $USER:$USER $venv_dir
sudo chmod -R 755 $work_dir