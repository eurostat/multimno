import sys

from components.ingestion.synthetic.event_synthetic import EventSynthetic
from components.execution.event_cleaning.event_cleaning import EventCleaning

from synthetic_data_generation.synthetic_events import SyntheticEvents
from synthetic_data_generation.synthetic_errors import SyntheticErrors


CONSTRUCTORS = {
    # SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    SyntheticErrors.COMPONENT_ID: SyntheticErrors,
}


if __name__ == "__main__":
    #component_id = sys.argv[1]
    #general_config_path = sys.argv[2]
    #component_config_path = sys.argv[3]
    
    component_id = "SyntheticErrorsGenerator"# sys.argv[1]
    general_config_path = "/opt/dev/src/synthetic_data_generation/config.ini" # sys.argv[2]
    component_config_path = "/opt/dev/src/synthetic_data_generation/config.ini" # sys.argv[3]
    
    component = CONSTRUCTORS[component_id](general_config_path, component_config_path)
    component.execute()
