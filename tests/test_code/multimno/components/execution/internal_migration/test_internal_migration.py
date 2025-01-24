from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames
from multimno.components.execution.internal_migration.internal_migration import InternalMigration

from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_internal_migration_quality_metrics_data_object import (
    SilverInternalMigrationQualityMetricsDataObject,
)

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.internal_migration.aux_internal_migration import (
    set_input_data,
    generate_expected_internal_migration_data,
    generate_expected_internal_migration_quality_metrics,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_internal_migration(spark):
    """
    Test the internal migration component.

    DESCRIPTION:
    This test verifies the internal migration component. It initialises the necessary configurations, sets up the
    input data, executes the internal migration process, and asserts that the output DataFrame matches the expected
    result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input UE label data and geozones using the test configuration
    4. Initialise the InternalMigration component with the test configuration
    5. Execute the internal migration component.
    6. Read the output of the internal migration component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/internal_migration/internal_migration.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config)

    # Initialise component
    migration = InternalMigration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    migration.execute()

    # Read output
    output_do = migration.output_data_objects[SilverInternalMigrationDataObject.ID]
    metrics_do = migration.output_data_objects[SilverInternalMigrationQualityMetricsDataObject.ID]
    output_do.read()
    metrics_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_internal_migration_data(), SilverInternalMigrationDataObject.SCHEMA
    )

    expected_metrics = spark.createDataFrame(
        generate_expected_internal_migration_quality_metrics(), SilverInternalMigrationQualityMetricsDataObject.SCHEMA
    )

    # Assert equality of internal migration results
    assertDataFrameEqual(output_do.df, expected_result)

    # Assert equality of internal migration quality metrics
    assertDataFrameEqual(output_do.df.drop(ColNames.result_timestamp), expected_result.drop(ColNames.result_timestamp))
