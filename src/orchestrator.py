import json
import os
import subprocess
import sys

if __name__ == "__main__":
    pipeline_config_path = sys.argv[1]

    if not os.path.exists(pipeline_config_path):
        print(f"Pipeline config path not found: {pipeline_config_path}", file=sys.stderr)
        sys.exit(1)

    with open(pipeline_config_path) as out:
        pipeline_config = json.load(out)

    general_config_path = pipeline_config["general_config_path"]

    for step in pipeline_config["pipeline"]:
        component_id = step["component_id"]
        component_config_path = step["component_config_path"]

        result = subprocess.run(
            ["spark-submit", f"src/main.py", component_id, general_config_path, component_config_path]
        )
        if result.returncode != 0:
            print(
                f"[X] ------ Component Error ------",
                f"Error executing component: {component_id}",
                f"General config: {general_config_path}",
                f"Component config: {component_config_path}",
                f"[X] -----------------------------",
                file=sys.stderr,
                sep=os.linesep,
            )
            sys.exit(1)
