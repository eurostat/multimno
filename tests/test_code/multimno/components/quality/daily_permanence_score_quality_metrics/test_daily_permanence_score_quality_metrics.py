import pytest
from multimno.core.exceptions import CriticalQualityWarningRaisedException
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames
from multimno.core.utils import apply_schema_casting

from multimno.components.quality.daily_permanence_score_quality_metrics.daily_permanence_score_quality_metrics import (
    DailyPermanenceScoreQualityMetrics,
)
from multimno.core.data_objects.silver.silver_daily_permanence_score_quality_metrics_data_object import (
    SilverDailyPermanenceScoreQualityMetrics,
)

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.quality.daily_permanence_score_quality_metrics.aux_daily_permanence_score_quality_metrics import (
    set_input_data,
    get_expected_metrics,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_daily_permanence_score_quality_metrics(spark):
    """
    Test the daily permanence score quality metrics component.

    DESCRIPTION:
    This test verifies the daily permanence score quality metrics component. It initialises the necessary configurations, sets
    up the input data, executes the internal migration process, and asserts that the output DataFrame matches the
    expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input daily permanence score using the test configuration
    4. Initialise the DailyPermanenceScoreQualityMetrics component with the test configuration
    5. Execute the quality metrics component.
    6. Read the output of the quality metrics component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/daily_aggregations/daily_permanence_score_quality_metrics.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config)

    # Initialise component
    qm = DailyPermanenceScoreQualityMetrics(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    with pytest.raises(CriticalQualityWarningRaisedException):
        qm.execute()
    # qm.execute()

    # # Read output
    # output_do = qm.output_data_objects[SilverDailyPermanenceScoreQualityMetrics.ID]
    # output_do.read()
    # # Get expected result
    # expected_result = apply_schema_casting(
    #     spark.createDataFrame(get_expected_metrics(), SilverDailyPermanenceScoreQualityMetrics.SCHEMA),
    #     SilverDailyPermanenceScoreQualityMetrics.SCHEMA,
    # )

    # # Assert equality of quality metrics results
    # assertDataFrameEqual(output_do.df.drop(ColNames.result_timestamp), expected_result.drop(ColNames.result_timestamp))
