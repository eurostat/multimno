#!/usr/bin/python

"""
Module that orchestrates MultiMNO pipeline components. A spark-submit will be performed for each 
component in the pipeline.

Usage: 

```
python multimno/orchestrator.py <pipeline.json>
```

- pipeline.json: Path to a json file with the pipeline configuration.
"""

import json
import os
import subprocess
import sys
import re

if __name__ == "__main__":
    pipeline_config_path = sys.argv[1]

    MAIN_PATH = "multimno/main.py"

    # Verify configuration files
    if not os.path.exists(pipeline_config_path):
        print(f"Pipeline config path not found: {pipeline_config_path}", file=sys.stderr)
        sys.exit(1)

    # Load pipeline
    try:
        with open(pipeline_config_path, encoding="utf-8") as out:
            pipeline_config = json.load(out)
    except json.decoder.JSONDecodeError:
        print(f"Pipeline JSON file could not be decoded.\nBad formatted file: {pipeline_config_path}.", file=sys.stderr)
        sys.exit(1)

    # Load general config
    general_config_path = pipeline_config["general_config_path"]

    # Load spark submit args
    spark_args = pipeline_config["spark_submit_args"]
    # Check for special characters
    SPECIAL_CHARS = r"[\`~!@#$%^&*()\+{}\[\]|;'\"<>?]"
    for s in spark_args:
        if re.search(SPECIAL_CHARS, s):
            print(f"Spark submit argument contains special characters: {s}", file=sys.stderr)
            sys.exit(1)

    spark_submit_command_base = ["spark-submit"] + spark_args

    # Start pipeline
    print("Starting pipeline...", flush=True)

    for step in pipeline_config["pipeline"]:
        component_id = step["component_id"]
        component_config_path = step["component_config_path"]

        # Set spark submit command
        spark_submit_suffix = [MAIN_PATH, component_id, general_config_path, component_config_path]
        spark_submit_command = spark_submit_command_base + spark_submit_suffix

        # Launch command
        result = subprocess.run(spark_submit_command, check=False)

        # Parse result
        if result.returncode != 0:
            print(
                "[X] ------ Component Error ------",
                f"Error executing component: {component_id}",
                f"General config: {general_config_path}",
                f"Component config: {component_config_path}",
                "[X] -----------------------------",
                file=sys.stderr,
                sep=os.linesep,
            )
            sys.exit(1)

    print("Pipeline finished successfully!")
