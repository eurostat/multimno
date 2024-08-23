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

import argparse

# Synthetic
from multimno.components.ingestion.synthetic.synthetic_network import SyntheticNetwork
from multimno.components.ingestion.synthetic.synthetic_diaries import SyntheticDiaries
from multimno.components.ingestion.synthetic.synthetic_events import SyntheticEvents

# Ingestion
from multimno.components.ingestion.grid_generation.inspire_grid_generation import (
    InspireGridGeneration,
)
from multimno.components.ingestion.spatial_data_ingestion.overture_data_ingestion import (
    OvertureDataIngestion,
)
from multimno.components.ingestion.spatial_data_ingestion.gisco_data_ingestion import (
    GiscoDataIngestion,
)

# Execution - Spatial
from multimno.components.execution.grid_enrichment.grid_enrichment import GridEnrichment
from multimno.components.execution.geozones_grid_mapping.geozones_grid_mapping import (
    GeozonesGridMapping,
)

# Execution - Events
from multimno.components.execution.event_cleaning.event_cleaning import EventCleaning
from multimno.components.execution.event_semantic_cleaning.event_semantic_cleaning import (
    SemanticCleaning,
)

# Execution - Network
from multimno.components.execution.network_cleaning.network_cleaning import (
    NetworkCleaning,
)
from multimno.components.execution.signal_strength.signal_stength_modeling import (
    SignalStrengthModeling,
)
from multimno.components.execution.cell_footprint.cell_footprint_estimation import (
    CellFootprintEstimation,
)
from multimno.components.execution.cell_connection_probability.cell_connection_probability import (
    CellConnectionProbabilityEstimation,
)

# Exection - Daily
from multimno.components.execution.time_segments.continuous_time_segmentation import (
    ContinuousTimeSegmentation,
)
from multimno.components.execution.daily_permanence_score.daily_permanence_score import (
    DailyPermanenceScore,
)
from multimno.components.execution.present_population.present_population_estimation import PresentPopulationEstimation

# Execution - Midterm
from multimno.components.execution.midterm_permanence_score.midterm_permanence_score import MidtermPermanenceScore

# Execution - Longterm
from multimno.components.execution.longterm_permanence_score.longterm_permanence_score import LongtermPermanenceScore
from multimno.components.execution.usual_environment_labeling.usual_environment_labeling import UsualEnvironmentLabeling
from multimno.components.execution.usual_environment_aggregation.usual_environment_aggregation import (
    UsualEnvironmentAggregation,
)

# Quality
from multimno.components.quality.event_quality_warnings.event_quality_warnings import (
    EventQualityWarnings,
)
from multimno.components.quality.semantic_quality_warnings.semantic_quality_warnings import (
    SemanticQualityWarnings,
)
from multimno.components.quality.network_quality_warnings.network_quality_warnings import (
    NetworkQualityWarnings,
)
from multimno.components.execution.device_activity_statistics.device_activity_statistics import (
    DeviceActivityStatistics,
)


CONSTRUCTORS = {
    # Synthetic generator
    SyntheticNetwork.COMPONENT_ID: SyntheticNetwork,
    SyntheticDiaries.COMPONENT_ID: SyntheticDiaries,
    SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    # Ingestion
    InspireGridGeneration.COMPONENT_ID: InspireGridGeneration,
    OvertureDataIngestion.COMPONENT_ID: OvertureDataIngestion,
    GiscoDataIngestion.COMPONENT_ID: GiscoDataIngestion,
    # Execution - Spatial
    GridEnrichment.COMPONENT_ID: GridEnrichment,
    GeozonesGridMapping.COMPONENT_ID: GeozonesGridMapping,
    # Execution - Events
    EventCleaning.COMPONENT_ID: EventCleaning,
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
    PresentPopulationEstimation.COMPONENT_ID: PresentPopulationEstimation,
    # Execution - Midterm
    MidtermPermanenceScore.COMPONENT_ID: MidtermPermanenceScore,
    # Execution - Longterm
    LongtermPermanenceScore.COMPONENT_ID: LongtermPermanenceScore,
    UsualEnvironmentLabeling.COMPONENT_ID: UsualEnvironmentLabeling,
    UsualEnvironmentAggregation.COMPONENT_ID: UsualEnvironmentAggregation,
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


def main():
    # Create the parser
    parser = argparse.ArgumentParser(
        description="Entrypoint of multimno software to launch a single component."
        + "Reference execution documentation: https://eurostat.github.io/multimno/latest/UserManual/execution"
    )

    # Add the arguments
    parser.add_argument("component_id", help="The component ID")
    parser.add_argument("general_config_path", help="The path to the general configuration file")
    parser.add_argument("component_config_path", help="The path to the component configuration file")

    # Parse the arguments
    args = parser.parse_args()

    # Use the arguments
    component = CONSTRUCTORS[args.component_id](args.general_config_path, args.component_config_path)
    component.execute()


if __name__ == "__main__":
    main()
