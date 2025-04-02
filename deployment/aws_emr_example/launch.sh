#!/bin/bash

set -e # Set exit on command failure

pipeline_path="$1"

cd /opt/workdir

# Run the orchestrator
/opt/venv/multimno/bin/python3 /opt/workdir/multimno/orchestrator_multimno.py $pipeline_path