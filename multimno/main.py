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

# Synthetic
from multimno.components.ingestion.synthetic.synthetic_network import SyntheticNetwork
from multimno.components.ingestion.synthetic.synthetic_diaries import SyntheticDiaries
from multimno.components.ingestion.synthetic.synthetic_events import SyntheticEvents
from multimno.components.ingestion.synthetic.synthetic_events_errors import SyntheticEventsErrors

# Ingestion
from multimno.components.ingestion.grid_generation.inspire_grid_generation import InspireGridGeneration
from multimno.components.execution.device_activity_statistics.device_activity_statistics import DeviceActivityStatistics

# Execution - Events
from multimno.components.execution.event_cleaning.event_cleaning import EventCleaning
from multimno.components.execution.event_deduplication.event_deduplication import EventDeduplication
from multimno.components.execution.event_semantic_cleaning.event_semantic_cleaning import SemanticCleaning

# Execution - Network
from multimno.components.execution.network_cleaning.network_cleaning import NetworkCleaning
from multimno.components.execution.signal_strength.signal_stength_modeling import SignalStrengthModeling
from multimno.components.execution.cell_footprint.cell_footprint_estimation import CellFootprintEstimation
from multimno.components.execution.cell_connection_probability.cell_connection_probability import (
    CellConnectionProbabilityEstimation,
)

# Exection - Daily
from multimno.components.execution.time_segments.continuous_time_segmentation import ContinuousTimeSegmentation
from multimno.components.execution.daily_permanence_score.daily_permanence_score import DailyPermanenceScore

# Quality
from multimno.components.quality.event_quality_warnings.event_quality_warnings import EventQualityWarnings
from multimno.components.quality.semantic_quality_warnings.semantic_quality_warnings import SemanticQualityWarnings
from multimno.components.quality.network_quality_warnings.network_quality_warnings import NetworkQualityWarnings


CONSTRUCTORS = {
    # Synthetic generator
    SyntheticNetwork.COMPONENT_ID: SyntheticNetwork,
    SyntheticDiaries.COMPONENT_ID: SyntheticDiaries,
    SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    SyntheticEventsErrors.COMPONENT_ID: SyntheticEventsErrors,
    # Ingestion
    InspireGridGeneration.COMPONENT_ID: InspireGridGeneration,
    # Execution - Events
    EventCleaning.COMPONENT_ID: EventCleaning,
    EventDeduplication.COMPONENT_ID: EventDeduplication,
    SemanticCleaning.COMPONENT_ID: SemanticCleaning,
    # Execution - Network
    NetworkCleaning.COMPONENT_ID: NetworkCleaning,
    DeviceActivityStatistics.COMPONENT_ID: DeviceActivityStatistics,
    SignalStrengthModeling.COMPONENT_ID: SignalStrengthModeling,
    CellFootprintEstimation.COMPONENT_ID: CellFootprintEstimation,
    CellConnectionProbabilityEstimation.COMPONENT_ID: CellConnectionProbabilityEstimation,
    # Execution - Daily
    ContinuousTimeSegmentation.COMPONENT_ID: ContinuousTimeSegmentation,
    DailyPermanenceScore.COMPONENT_ID: DailyPermanenceScore,
    # Quality
    EventQualityWarnings.COMPONENT_ID: EventQualityWarnings,
    SemanticQualityWarnings.COMPONENT_ID: SemanticQualityWarnings,
    NetworkQualityWarnings.COMPONENT_ID: NetworkQualityWarnings,
}


def build(component_id: str, general_config_path: str, component_config_path: str):
    """


    Args:
        component_id (str): id of the component
        general_config_path (str): general config path
        component_config_path (str): component config path

    Raises:
        ValueError: If the component_id is not supported.

    Returns:
        (multimno.core.component.Component): Component constructor.
    """
    try:
        constructor = CONSTRUCTORS[component_id]
    except KeyError as e:
        raise ValueError(f"Component {component_id} is not supported.") from e

    return constructor(general_config_path, component_config_path)


if __name__ == "__main__":
    component_id = sys.argv[1]
    general_config_path = sys.argv[2]
    component_config_path = sys.argv[3]

    component = CONSTRUCTORS[component_id](general_config_path, component_config_path)
    component.execute()
