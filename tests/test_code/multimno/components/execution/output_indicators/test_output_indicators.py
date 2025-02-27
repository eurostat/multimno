from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.components.execution.output_indicators.output_indicators import OutputIndicators

from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.output_indicators.aux_output_indicators import (
    set_input_data,
    generate_expected_obfuscated_internal_migration_data,
    generate_expected_deleted_internal_migration_data,
    generate_expected_obfuscated_present_population_zone_data,
    generate_expected_deleted_present_population_zone_data,
    generate_expected_obfuscated_present_population_100m_data,
    generate_expected_deleted_present_population_1km_data,
    generate_expected_deleted_usual_environment_100m_data,
    generate_expected_deleted_usual_environment_zone_data,
    generate_expected_obfuscated_usual_environment_1km_data,
    generate_expected_obfuscated_usual_environment_zone_data,
    generate_expected_obfuscated_inbound_tourism_departures_data,
    generate_expected_inbound_tourism_averages_data,
    generate_expected_deleted_outbound_tourism_data,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_internal_migration_output_indicators_obfuscate(spark):
    """
    Test the output indicators of internal migration.

    DESCRIPTION:
    This test verifies the output indicators component for internal migration data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_internal_migration_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "InternalMigration")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output
    output_do = indicators.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_internal_migration_data(), SilverInternalMigrationDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_internal_migration_output_indicators_delete(spark):
    """
    Test the output indicators of internal migration.

    DESCRIPTION:
    This test verifies the output indicators component for internal migration data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_internal_migration_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "InternalMigration")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output
    output_do = indicators.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_internal_migration_data(), SilverInternalMigrationDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_present_population_zone_output_indicators_obfuscate(spark):
    """
    Test the output indicators of present population.

    DESCRIPTION:
    This test verifies the output indicators component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_present_population_zone_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "PresentPopulationEstimation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverPresentPopulationZoneDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverPresentPopulationZoneDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_present_population_zone_data(), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_present_population_100m_output_indicators_obfuscate(spark):
    """
    Test the output indicators of present population.

    DESCRIPTION:
    This test verifies the output indicators component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_present_population_100m_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "PresentPopulationEstimation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverPresentPopulationZoneDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverPresentPopulationZoneDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_present_population_100m_data(), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_present_population_1km_output_indicators_delete(spark):
    """
    Test the output indicators of present population.

    DESCRIPTION:
    This test verifies the output indicators component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_present_population_1km_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "PresentPopulationEstimation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverPresentPopulationZoneDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverPresentPopulationZoneDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_present_population_1km_data(), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_present_population_zone_output_indicators_delete(spark):
    """
    Test the output indicators of present population.

    DESCRIPTION:
    This test verifies the output indicators component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_present_population_zone_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "PresentPopulationEstimation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverPresentPopulationZoneDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverPresentPopulationZoneDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_present_population_zone_data(), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_usual_environment_zone_output_indicators_delete(spark):
    """
    Test the output indicators of usual environment.

    DESCRIPTION:
    This test verifies the output indicators component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input usual environment data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_usual_environment_zone_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "UsualEnvironmentAggregation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverAggregatedUsualEnvironmentsZonesDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverAggregatedUsualEnvironmentsZonesDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_usual_environment_zone_data(), SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_usual_environment_zone_output_indicators_obfuscate(spark):
    """
    Test the output indicators of usual environment.

    DESCRIPTION:
    This test verifies the output indicators component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input usual environment data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_usual_environment_zone_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "UsualEnvironmentAggregation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverAggregatedUsualEnvironmentsZonesDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverAggregatedUsualEnvironmentsZonesDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_usual_environment_zone_data(),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_usual_environment_1km_output_indicators_obfuscate(spark):
    """
    Test the output indicators of usual environment.

    DESCRIPTION:
    This test verifies the output indicators component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input usual environment data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_usual_environment_1km_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "UsualEnvironmentAggregation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverAggregatedUsualEnvironmentsZonesDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverAggregatedUsualEnvironmentsZonesDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_obfuscated_usual_environment_1km_data(),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_usual_environment_100m_output_indicators_delete(spark):
    """
    Test the output indicators of usual environment.

    DESCRIPTION:
    This test verifies the output indicators component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input usual environment data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_usual_environment_100m_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "UsualEnvironmentAggregation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    if SilverAggregatedUsualEnvironmentsZonesDataObject.ID in indicators.output_data_objects:
        output_do = indicators.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    else:
        for key in indicators.output_data_objects.keys():
            if key.startswith(SilverAggregatedUsualEnvironmentsZonesDataObject.ID):
                break
        else:
            raise BaseException()

        output_do = indicators.output_data_objects[key]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_usual_environment_100m_data(), SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_inbound_tourism_output_indicators_obfuscate(spark):
    """
    Test the output indicators of inbound tourism.

    DESCRIPTION:
    This test verifies the output indicators component for inbound tourism data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input inbound tourism data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_inbound_tourism_obfuscate.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "TourismStatisticsCalculation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    output_dep_do = indicators.output_data_objects[SilverTourismZoneDeparturesNightsSpentDataObject.ID]
    output_avg_do = indicators.output_data_objects[SilverTourismTripAvgDestinationsNightsSpentDataObject.ID]
    output_dep_do.read()
    output_avg_do.read()

    # Get expected result
    expected_dep_result = spark.createDataFrame(
        generate_expected_obfuscated_inbound_tourism_departures_data(),
        SilverTourismZoneDeparturesNightsSpentDataObject.SCHEMA,
    )

    expected_avg_result = spark.createDataFrame(
        generate_expected_inbound_tourism_averages_data(), SilverTourismTripAvgDestinationsNightsSpentDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_dep_do.df, expected_dep_result)

    assertDataFrameEqual(output_avg_do.df, expected_avg_result)


def test_outbound_tourism_output_indicators_delete(spark):
    """
    Test the output indicators of outbound tourism.

    DESCRIPTION:
    This test verifies the output indicators component for outbound tourism data. It initialises the necessary
    configurations, sets up the input data, executes the output indicators process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input outbound tourism data using the test configuration
    4. Initialise the OutputIndicators component with the test configuration
    5. Execute the output indicators component.
    6. Read the output of the output indicators component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/output_indicators/output_indicators_outbound_tourism_delete.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Create input data
    set_input_data(spark, config, "TourismOutboundStatisticsCalculation")

    # Initialise component
    indicators = OutputIndicators(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    indicators.execute()

    # Read output

    output_do = indicators.output_data_objects[SilverTourismOutboundNightsSpentDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_deleted_outbound_tourism_data(), SilverTourismOutboundNightsSpentDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)
