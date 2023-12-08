import sys

from components.ingestion.synthetic.event_synthetic import EventSynthetic
from components.execution.event_cleaning.event_cleaning import EventCleaning

CONSTRUCTORS = {
    EventSynthetic.COMPONENT_ID: EventSynthetic,
    EventCleaning.COMPONENT_ID: EventCleaning,
}


if __name__ == "__main__":
    component_id = sys.argv[1]
    general_config_path = sys.argv[2]
    component_config_path = sys.argv[3]

    component = CONSTRUCTORS[component_id](general_config_path, component_config_path)
    component.execute()
