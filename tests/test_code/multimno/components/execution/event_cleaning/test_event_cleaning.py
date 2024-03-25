from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from pyspark.testing.utils import assertDataFrameEqual
from multimno.core.configuration import parse_configuration

from multimno.core.constants.columns import ColNames
from multimno.components.execution.event_cleaning.event_cleaning import EventCleaning
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir
from tests.test_code.multimno.components.execution.event_cleaning.aux_event_testing import (
    expected_events,
    expected_frequency_distribution,
    expected_quality_metrics_by_column,
    write_input_event_data,
)


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_events, expected_frequency_distribution, expected_quality_metrics_by_column]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_event_cleaning(spark, expected_events, expected_frequency_distribution, expected_quality_metrics_by_column):
    # Setup
    # define config path
    component_config_path = f"{TEST_RESOURCES_PATH}/config/event/event_cleaning/testing_event_cleaning.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    write_input_event_data(spark, config)

    # Init component class
    event_cleaning = EventCleaning(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    event_cleaning.execute()

    # Read results
    event_cleaning.output_data_objects[SilverEventDataObject.ID].read()
    event_cleaning.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID].read()
    event_cleaning.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID].read()
    result_events = event_cleaning.output_data_objects[SilverEventDataObject.ID].df
    result_frequency_distribution = event_cleaning.output_data_objects[
        SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID
    ].df
    result_quality_metrics = event_cleaning.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID].df

    # No reason to compare this
    result_quality_metrics = result_quality_metrics.drop(ColNames.result_timestamp)

    # Assertion
    assertDataFrameEqual(expected_events, result_events)
    assertDataFrameEqual(expected_frequency_distribution, result_frequency_distribution)
    assertDataFrameEqual(
        expected_quality_metrics_by_column.select(result_quality_metrics.columns), result_quality_metrics
    )
