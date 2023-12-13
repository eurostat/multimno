import sys

from components.ingestion.synthetic.synthetic_events import SyntheticEvents
from components.ingestion.synthetic.synthetic_errors import SyntheticErrors

from components.execution.event_cleaning.event_cleaning import EventCleaning

CONSTRUCTORS = {
    SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    SyntheticErrors.COMPONENT_ID: SyntheticErrors,
}


if __name__ == "__main__":
    # component_id = sys.argv[1]
    general_config_path = "/opt/dev/pipe_configs/configurations/synthetic_events/synth_config.ini" #sys.argv[1] # 
    component_config_path =  "/opt/dev/pipe_configs/configurations/synthetic_events/synth_config.ini"  # #sys.argv[2]

    for component_id in CONSTRUCTORS.keys():
        component = CONSTRUCTORS[component_id](general_config_path, component_config_path)
        component.execute()
