from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.components.execution.kanonimity.kanonimity import KAnonimity

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)

from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.kanonimity.aux_kanonimity import (
    set_input_data,
    generate_expected_deleted_aggregated_ue_zone_data,
    generate_expected_deleted_present_population_zones_data,
    generate_expected_deleted_internal_migration_zones_data,
    generate_expected_obfuscated_aggregated_ue_zone_data,
    generate_expected_obfuscated_present_population_zones_data,
    generate_expected_obfuscated_internal_migration_data,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_obfuscated_present_population_zone(spark):
    """
    Test the obfuscation of present population data at zone level.

    DESCRIPTION:
    This test verifies the k-anonimity component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_obfuscated_present_population.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    ts = "2023-01-01T00:00:00"
    # Create input data
    set_input_data(spark, config, "present_population", timestamp=ts)

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_present_population_zones_data(ts), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_deleted_present_population_zone(spark):
    """
    Test the deletion present population data at zone level.

    DESCRIPTION:
    This test verifies the k-anonimity component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_deleted_present_population.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    ts = "2023-01-01T00:00:00"
    # Create input data
    set_input_data(spark, config, "present_population", timestamp=ts)

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_present_population_zones_data(ts), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_obfuscated_aggregated_usual_environment_zone(spark):
    """
    Test the k-anonimity of usual environment data at zone level, with obfuscation and k=15.

    DESCRIPTION:
    This test verifies the k-anonimity component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_obfuscated_usual_environment.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    date = "2023-01-01"
    # Create input data
    set_input_data(spark, config, "usual_environment", date=date)

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_aggregated_ue_zone_data(date),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_deleted_aggregated_usual_environment_zone(spark):
    """
    Test the k-anonimity of usual environment data at zone level, with deletion and k=15.

    DESCRIPTION:
    This test verifies the k-anonimity component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_deleted_usual_environment.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    date = "2023-01-01"
    # Create input data
    set_input_data(spark, config, "usual_environment", date=date)

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_aggregated_ue_zone_data(date),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_obfuscated_internal_migration(spark):
    """
    Test the k-anonimity of internal migration data , with obfuscation and k=15.

    DESCRIPTION:
    This test verifies the k-anonimity component for interal migration data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_obfuscated_internal_migration.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "internal_migration")

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_internal_migration_data(),
        SilverInternalMigrationDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_deleted_internal_migration(spark):
    """
    Test the k-anonimity of internal migration data at zone level, with deletion and k=15.

    DESCRIPTION:
    This test verifies the k-anonimity component for interna migration data. It initialises the necessary
    configurations, sets up the input data, executes the k-anonimity process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the KAnonimity component with the test configuration
    5. Execute the k-anonimity component.
    6. Read the output of the k-anonimity component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = f"{TEST_RESOURCES_PATH}/config/kanonimity/kanonimity_deleted_internal_migration.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "internal_migration")

    # Initialise component
    kanonimity = KAnonimity(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    kanonimity.execute()

    # Read output
    output_do = kanonimity.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_internal_migration_zones_data(),
        SilverInternalMigrationDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)
