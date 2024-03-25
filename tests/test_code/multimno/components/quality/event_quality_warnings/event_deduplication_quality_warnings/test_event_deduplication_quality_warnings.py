from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration

from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_warnings_log_table import (
    SilverEventDataSyntacticQualityWarningsLogTable,
)
from multimno.components.quality.event_quality_warnings.event_quality_warnings import EventQualityWarnings

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.quality.event_quality_warnings.event_deduplication_quality_warnings.aux_event_deduplication_quality_warnings_testing import (
    expected_event_deduplication_qw_log_table,
)
from tests.test_code.multimno.components.quality.event_quality_warnings.event_deduplication_quality_warnings.aux_event_deduplication_quality_warnings_testing import (
    set_input_event_deduplication_qm_freq_distr,
    set_input_event_deduplication_qm_by_column,
)
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_event_deduplication_qw_log_table]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_event_deduplication_quality_warnings(spark, expected_event_deduplication_qw_log_table):
    """
    DESCRIPTION:
        Test shall execute the EventQualityWarnings component and produce only LogTable with two rows,
        which indicates two QWs - error_type_rate_upper_variability and error_type_rate_upper_limit for
        same locations duplicates error.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/event/event_deduplication_quality_warnings/testing_event_deduplication_quality_warnings.ini
        Input Data:
            silver_event_deduplication_qm_freq_distr: ${Paths:silver_quality_metrics_dir}/event_deduplicated_quality_metrics_frequency_distribution
            silver_event_deduplication_qm_by_column: ${Paths:silver_quality_metrics_dir}/event_deduplicated_quality_metrics_by_column

    EXPECTED OUTPUT:
        silver_event_deduplication_log_table: ${Paths:silver_quality_warnings_dir}/event_deduplicated_quality_warnings_log_table

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the EventQualityWarnings component with test configs.
        4.- Execute the EventQualityWarnings.
        5.- Read written data in /opt/testing_data with SilverEventDataSyntacticQualityWarningsLogTable class.
        6.- Assert DataFrames are equal.
    """
    # Setup

    # Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/event/event_deduplication_quality_warnings/testing_event_deduplication_quality_warnings.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create Input data
    set_input_event_deduplication_qm_freq_distr(spark, config)
    set_input_event_deduplication_qm_by_column(spark, config)

    # Expected (defined as fixture)

    # Init component class
    event_quality_warnings = EventQualityWarnings(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    event_quality_warnings.execute()

    # Assertion
    # read from test data output
    output_data_object_log_table = event_quality_warnings.output_qw_data_objects[
        SilverEventDataSyntacticQualityWarningsLogTable.ID
    ]
    output_data_object_log_table.read()
    log_table = output_data_object_log_table.df.sort([ColNames.measure_definition, ColNames.date])
    log_table = log_table.select(SilverEventDataSyntacticQualityWarningsLogTable.SCHEMA.fieldNames())
    # assert read data == expected
    assertDataFrameEqual(log_table, expected_event_deduplication_qw_log_table)
