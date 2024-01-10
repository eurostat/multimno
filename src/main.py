"""
Application entrypoint for launching a single component.
"""

import sys

from components.execution.event_cleaning.event_cleaning import EventCleaning
from components.ingestion.synthetic.synthetic_events import SyntheticEvents

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
