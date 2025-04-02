#!/bin/bash


set -e

# Install cyclonedx-bom in the devcontainer
python -m pip install cyclonedx-bom
# Step 1: Create the virtual environment without pip
python3 -m venv /opt/venv/sbom/ --copies --without-pip
# Step 2: Activate the virtual environment
source /opt/venv/sbom/bin/activate
# Step 3: Manually install pip
curl https://bootstrap.pypa.io/get-pip.py | python3
# Step 4: Install the CycloneDX plugin + multimno dependencies
pip install .[spark]
# Step 5: Deactivate the virtual environment
deactivate
# Step 6: Generate the SBOM
mkdir -p /opt/app/license
cyclonedx-py environment "/opt/venv/sbom/bin/python3" > /opt/app/license/sbom.json
# Step 7: Remove the virtual environment
rm -rf /opt/venv/sbom/

# Step 8: Generate concise license data
python3 /opt/app/resources/scripts/license/generate_concise_license_data.py

