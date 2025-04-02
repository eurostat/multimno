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
import sys

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
from multimno.components.ingestion.spatial_data_ingestion.overture_data_transformation import (
    OvertureDataTransformation,
)
from multimno.components.ingestion.spatial_data_ingestion.gisco_data_ingestion import (
    GiscoDataIngestion,
)

from multimno.components.ingestion.data_filtering.data_filtering import DataFiltering

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
from multimno.components.execution.cell_footprint.cell_footprint_estimation import (
    CellFootprintEstimation,
)
from multimno.components.execution.cell_connection_probability.cell_connection_probability import (
    CellConnectionProbabilityEstimation,
)
from multimno.components.execution.cell_proximity_estimation.cell_proximity_estimation import (
    CellProximityEstimation,
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
from multimno.components.execution.internal_migration.internal_migration import InternalMigration

# Postprocessing
from multimno.components.execution.output_indicators.output_indicators import OutputIndicators
from multimno.components.execution.multimno_aggregation.multimno_aggregation import MultiMNOAggregation

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
from multimno.components.quality.cell_footprint_quality_metrics.cell_footprint_quality_metrics import (
    CellFootPrintQualityMetrics,
)
from multimno.components.quality.daily_permanence_score_quality_metrics.daily_permanence_score_quality_metrics import (
    DailyPermanenceScoreQualityMetrics,
)

# Tourism
from multimno.components.execution.tourism_stays_estimation.tourism_stays_estimation import TourismStaysEstimation
from multimno.components.execution.tourism_statistics.tourism_statistics_calculation import (
    TourismStatisticsCalculation,
)
from multimno.components.execution.tourism_outbound_statistics.tourism_outbound_statistics_calculation import (
    TourismOutboundStatisticsCalculation,
)
from multimno.core.exceptions import ComponentNotSupported, CriticalQualityWarningRaisedException
from multimno.core.settings import QUALITY_WARNINGS_EXIT_CODE, ERROR_EXIT_CODE

CONSTRUCTORS = {
    # Synthetic generator
    SyntheticNetwork.COMPONENT_ID: SyntheticNetwork,
    SyntheticDiaries.COMPONENT_ID: SyntheticDiaries,
    SyntheticEvents.COMPONENT_ID: SyntheticEvents,
    # Ingestion
    InspireGridGeneration.COMPONENT_ID: InspireGridGeneration,
    OvertureDataIngestion.COMPONENT_ID: OvertureDataIngestion,
    OvertureDataTransformation.COMPONENT_ID: OvertureDataTransformation,
    GiscoDataIngestion.COMPONENT_ID: GiscoDataIngestion,
    DataFiltering.COMPONENT_ID: DataFiltering,
    # Execution - Spatial
    GridEnrichment.COMPONENT_ID: GridEnrichment,
    GeozonesGridMapping.COMPONENT_ID: GeozonesGridMapping,
    # Execution - Events
    EventCleaning.COMPONENT_ID: EventCleaning,
    SemanticCleaning.COMPONENT_ID: SemanticCleaning,
    # Execution - Network
    NetworkCleaning.COMPONENT_ID: NetworkCleaning,
    DeviceActivityStatistics.COMPONENT_ID: DeviceActivityStatistics,
    CellFootprintEstimation.COMPONENT_ID: CellFootprintEstimation,
    CellConnectionProbabilityEstimation.COMPONENT_ID: CellConnectionProbabilityEstimation,
    CellProximityEstimation.COMPONENT_ID: CellProximityEstimation,
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
    InternalMigration.COMPONENT_ID: InternalMigration,
    # Tourism
    TourismStaysEstimation.COMPONENT_ID: TourismStaysEstimation,
    TourismStatisticsCalculation.COMPONENT_ID: TourismStatisticsCalculation,
    TourismOutboundStatisticsCalculation.COMPONENT_ID: TourismOutboundStatisticsCalculation,
    # Quality
    EventQualityWarnings.COMPONENT_ID: EventQualityWarnings,
    SemanticQualityWarnings.COMPONENT_ID: SemanticQualityWarnings,
    NetworkQualityWarnings.COMPONENT_ID: NetworkQualityWarnings,
    CellFootPrintQualityMetrics.COMPONENT_ID: CellFootPrintQualityMetrics,
    DailyPermanenceScoreQualityMetrics.COMPONENT_ID: DailyPermanenceScoreQualityMetrics,
    # Postprocessing (Export)
    OutputIndicators.COMPONENT_ID: OutputIndicators,
    MultiMNOAggregation.COMPONENT_ID: MultiMNOAggregation,
}


def build(component_id: str, general_config_path: str, component_config_path: str):
    """
    Build a component based on the component_id.

    Args:
        component_id (str): id of the component
        general_config_path (str): general config path
        component_config_path (str): component config path

    Raises:
        ComponentNotSupported: If the component_id is not supported.

    Returns:
        (multimno.core.component.Component): Component constructor.
    """
    constructor = CONSTRUCTORS.get(component_id)
    if constructor is None:
        raise ComponentNotSupported(component_id)

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

    # Build component
    try:
        component = build(args.component_id, args.general_config_path, args.component_config_path)
    except ComponentNotSupported as e:
        print(e)
        sys.exit(ERROR_EXIT_CODE)
    except Exception as e:
        print(f"An unexpected error occurred during the creation of the component: {e}")
        sys.exit(ERROR_EXIT_CODE)

    # Execute component
    try:
        component.execute()
    except CriticalQualityWarningRaisedException as e:
        component.logger.error(e)
        sys.exit(QUALITY_WARNINGS_EXIT_CODE)
    except Exception as e:
        component.logger.error(f"An unexpected error occurred during the execution of the component: {e}")
        raise (e)
        # sys.exit(ERROR_EXIT_CODE) # TO be changed in production 1.0


if __name__ == "__main__":
    main()
