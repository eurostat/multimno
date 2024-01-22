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

if __name__ == "__main__":
    pipeline_config_path = sys.argv[1]

    MAIN_PATH = "multimno/main.py"

    if not os.path.exists(pipeline_config_path):
        print(f"Pipeline config path not found: {pipeline_config_path}", file=sys.stderr)
        sys.exit(1)

    with open(pipeline_config_path, encoding="utf-8") as out:
        pipeline_config = json.load(out)

    general_config_path = pipeline_config["general_config_path"]

    for step in pipeline_config["pipeline"]:
        component_id = step["component_id"]
        component_config_path = step["component_config_path"]

        result = subprocess.run(
            ["spark-submit", MAIN_PATH, component_id, general_config_path, component_config_path], check=False
        )
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
