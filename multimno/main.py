"""
Application entrypoint for launching a single component.

Usage:

```
python multimno/main.py <component_id> <general_config_path> <component_config_path>
```

- component_id: ID of the component to be executed.
- general_config_path: Path to a INI file with the general configuration of the execution.
- component_config_path: Path to a INI file with the specific configuration of the component.
"""

import sys

from multimno.components.execution.event_cleaning.event_cleaning import EventCleaning
from multimno.components.ingestion.synthetic.synthetic_events import SyntheticEvents

CONSTRUCTORS = {
    SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    EventCleaning.COMPONENT_ID: EventCleaning,
}


if __name__ == "__main__":
    component_id = sys.argv[1]
    general_config_path = sys.argv[2]
    component_config_path = sys.argv[3]

    component = CONSTRUCTORS[component_id](general_config_path, component_config_path)
    component.execute()
