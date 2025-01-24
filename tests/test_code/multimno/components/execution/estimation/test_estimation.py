from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from multimno.core.configuration import parse_configuration
from multimno.components.execution.estimation.estimation import Estimation

from multimno.core.data_objects.silver.silver_present_population_data_object import SilverPresentPopulationDataObject
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)

from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.estimation.aux_estimation import set_input_data

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.estimation.aux_estimation import (
    generate_expected_estimated_aggregated_ue_zone_data,
    generate_expected_estimated_present_population_zones_data,
    generate_expected_internal_migration_data,
    generate_expected_estimated_aggregated_ue_100m_data,
    generate_expected_estimated_present_population_100m_data,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_present_population_zone_estimation(spark):
    """
    Test the estimation of present population data at zone level.

    DESCRIPTION:
    This test verifies the estimation component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the estimation process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the Estimation component with the test configuration
    5. Execute the estimation component.
    6. Read the output of the estimation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/estimation/estimation_present_population.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    ts = "2023-01-01T00:00:00"
    # Create input data
    set_input_data(spark, config, "present_population", timestamp=ts)

    # Initialise component
    estimation = Estimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    estimation.execute()

    # Read output
    output_do = estimation.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_estimated_present_population_zones_data(ts), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_present_population_100m_estimation(spark):
    """
    Test the estimation of present population data at 100m grid level.

    DESCRIPTION:
    This test verifies the estimation component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the estimation process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the Estimation component with the test configuration
    5. Execute the estimation component.
    6. Read the output of the estimation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/estimation/estimation_present_population_100m_grid.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    ts = "2023-01-01T00:00:00"
    # Create input data
    set_input_data(spark, config, "present_population", at_grid_level=True, timestamp=ts)

    # Initialise component
    estimation = Estimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    estimation.execute()

    # Read output
    output_do = estimation.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_estimated_present_population_100m_data(ts), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_aggregated_usual_environment_zone_estimation(spark):
    """
    Test the estimation of usual environment data at zone level.

    DESCRIPTION:
    This test verifies the estimation component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the estimation process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the Estimation component with the test configuration
    5. Execute the estimation component.
    6. Read the output of the estimation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/estimation/estimation_usual_environment.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    date = "2023-01-01"
    # Create input data
    set_input_data(spark, config, "usual_environment", date=date)

    # Initialise component
    estimation = Estimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    estimation.execute()

    # Read output
    output_do = estimation.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_estimated_aggregated_ue_zone_data(date),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_aggregated_usual_environment_100m_estimation(spark):
    """
    Test the estimation of usual environment data at 100m grid level.

    DESCRIPTION:
    This test verifies the estimation component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the estimation process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the Estimation component with the test configuration
    5. Execute the estimation component.
    6. Read the output of the estimation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/estimation/estimation_usual_environment_100m_grid.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    date = "2023-01-01"
    # Create input data
    set_input_data(spark, config, "usual_environment", at_grid_level=True, date=date)

    # Initialise component
    estimation = Estimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    estimation.execute()

    # Read output
    output_do = estimation.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_estimated_aggregated_ue_100m_data(date),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_internal_migration_estimation(spark):
    """
    Test the estimation of internal migration data.

    DESCRIPTION:
    This test verifies the estimation component for internal migration data. It initialises the necessary
    configurations, sets up the input data, executes the internal migration process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the Estimation component with the test configuration
    5. Execute the estimation component.
    6. Read the output of the estimation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/estimation/estimation_internal_migration.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "internal_migration")

    # Initialise component
    estimation = Estimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    estimation.execute()

    # Read output
    output_do = estimation.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_internal_migration_data(), SilverInternalMigrationDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)
