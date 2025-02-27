from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.components.execution.multimno_aggregation.multimno_aggregation import MultiMNOAggregation

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)

from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.multimno_aggregation.aux_multimno_aggregation import (
    set_input_data,
    generate_expected_aggregated_ue_zone_data,
    generate_expected_present_population_zones_data,
    generate_expected_internal_migration_zones_data,
    generate_expected_inbound_tourism_data,
    generate_expected_outbound_tourism_data,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_multimno_aggregated_present_population_zone(spark):
    """
    Test the MultiMNO aggregation of present population data at zone level.

    DESCRIPTION:
    This test verifies the MultiMNO aggregation component for present population data. It initialises the necessary
    configurations, sets up the input data, executes the aggregation process, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the MultiMNOAggregation component with the test configuration
    5. Execute the aggregation component.
    6. Read the output of the aggregation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/multimno_aggregation/multimno_aggregation_present_population.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    ts = "2023-01-01T00:00:00"
    # Create input data
    set_input_data(spark, config, "PresentPopulationEstimation", timestamp=ts)

    # Initialise component
    multimno_agg = MultiMNOAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    multimno_agg.execute()

    # Read output
    output_do = multimno_agg.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_present_population_zones_data(ts), SilverPresentPopulationZoneDataObject.SCHEMA
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_multimno_aggregated_usual_environment_zone(spark):
    """
    Test the MultiMNO aggregation of usual environment data at zone level.

    DESCRIPTION:
    This test verifies MultiMNO aggregation component for usual environment data. It initialises the necessary
    configurations, sets up the input data, executes the aggregation porcess, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input present population data using the test configuration
    4. Initialise the MultiMNOAggregation component with the test configuration
    5. Execute the aggregation component.
    6. Read the output of the aggregation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/multimno_aggregation/multimno_aggregation_usual_environment.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    date = "2023-01-01"
    # Create input data
    set_input_data(spark, config, "UsualEnvironmentAggregation", date=date)

    # Initialise component
    multimno_agg = MultiMNOAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    multimno_agg.execute()

    # Read output
    output_do = multimno_agg.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_aggregated_ue_zone_data(date),
        SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_multimno_internal_migration(spark):
    """
    Test the MultiMNO aggregation of internal migration.

    DESCRIPTION:
    This test verifies MultiMNO aggregation component for internal migration data. It initialises the necessary
    configurations, sets up the input data, executes the aggregation porcess, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input internal migration data using the test configuration
    4. Initialise the MultiMNOAggregation component with the test configuration
    5. Execute the aggregation component.
    6. Read the output of the aggregation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/multimno_aggregation/multimno_aggregation_internal_migration.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "InternalMigration")

    # Initialise component
    multimno_agg = MultiMNOAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    multimno_agg.execute()

    # Read output
    output_do = multimno_agg.output_data_objects[SilverInternalMigrationDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_internal_migration_zones_data(),
        SilverInternalMigrationDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_multimno_inbound_tourism(spark):
    """
    Test the MultiMNO aggregation of inbound tourism.

    DESCRIPTION:
    This test verifies MultiMNO aggregation component for inbound tourism data. It initialises the necessary
    configurations, sets up the input data, executes the aggregation porcess, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input inbound tourism data using the test configuration
    4. Initialise the MultiMNOAggregation component with the test configuration
    5. Execute the aggregation component.
    6. Read the output of the aggregation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/multimno_aggregation/multimno_aggregation_inbound_tourism.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "TourismStatisticsCalculation")

    # Initialise component
    multimno_agg = MultiMNOAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    multimno_agg.execute()

    # Read output
    output_do = multimno_agg.output_data_objects[SilverTourismZoneDeparturesNightsSpentDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_inbound_tourism_data(),
        SilverTourismZoneDeparturesNightsSpentDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)


def test_multimno_outbound_tourism(spark):
    """
    Test the MultiMNO aggregation of outbound tourism.

    DESCRIPTION:
    This test verifies MultiMNO aggregation component for outbound tourism data. It initialises the necessary
    configurations, sets up the input data, executes the aggregation porcess, and asserts that the output
    DataFrame matches the expected result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input outbound tourism data using the test configuration
    4. Initialise the MultiMNOAggregation component with the test configuration
    5. Execute the aggregation component.
    6. Read the output of the aggregation component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/multimno_aggregation/multimno_aggregation_outbound_tourism.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config, "TourismOutboundStatisticsCalculation")

    # Initialise component
    multimno_agg = MultiMNOAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    multimno_agg.execute()

    # Read output
    output_do = multimno_agg.output_data_objects[SilverTourismOutboundNightsSpentDataObject.ID]
    output_do.read()

    # Get expected result
    expected_result = spark.createDataFrame(
        generate_expected_outbound_tourism_data(),
        SilverTourismOutboundNightsSpentDataObject.SCHEMA,
    )

    # Assert equality of outputs
    assertDataFrameEqual(output_do.df, expected_result)
