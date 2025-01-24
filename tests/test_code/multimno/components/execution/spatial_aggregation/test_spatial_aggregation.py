from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from multimno.core.configuration import parse_configuration
from multimno.core.utils import apply_schema_casting
from multimno.components.execution.spatial_aggregation.spatial_aggregation import SpatialAggregation

from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_data_object import (
    SilverPresentPopulationDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.spatial_aggregation.aux_spatial_aggregation import (
    generate_input_population_grid_data,
    generate_input_ue_grid_data,
    generate_zone_to_grid_map_data,
    generate_expected_population_zone_data,
    generate_expected_ue_zone_data,
    generate_expected_population_1km_grid_data,
    generate_expected_ue_1km_grid_data,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def set_grid_zone_map_data(spark: SparkSession):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
    """

    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)
    grid_zone_map_data_path = config["Paths.Silver"]["geozones_grid_map_data_silver"]
    grid_zone_map_do = SilverGeozonesGridMapDataObject(spark, grid_zone_map_data_path)
    grid_zone_map_do.df = spark.createDataFrame(
        generate_zone_to_grid_map_data("2023-01-01"), schema=SilverGeozonesGridMapDataObject.SCHEMA
    )
    grid_zone_map_do.write()


def set_population_test_data(spark: SparkSession):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
    """

    # Set input population data
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)
    input_population_data_path = config["Paths.Silver"]["present_population_silver"]
    input_population_data_do = SilverPresentPopulationDataObject(spark, input_population_data_path)
    input_population_data_do.df = spark.createDataFrame(
        generate_input_population_grid_data("2023-01-01T00:00:00"), schema=SilverPresentPopulationDataObject.SCHEMA
    )
    input_population_data_do.write()


def set_ue_test_data(spark: SparkSession):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
    """

    # Set input UE data
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)
    input_ue_data_path = config["Paths.Silver"]["aggregated_usual_environments_silver"]
    input_ue_data_do = SilverAggregatedUsualEnvironmentsDataObject(spark, input_ue_data_path)
    input_ue_data_do.df = spark.createDataFrame(
        generate_input_ue_grid_data("2023-01", "2023-03"), schema=SilverAggregatedUsualEnvironmentsDataObject.SCHEMA
    )
    input_ue_data_do.write()


def test_present_population_spatial_aggregation(spark):
    """
    Test the spatial aggregation of present population data.

    DESCRIPTION:
    This test verifies the spatial aggregation component for present population data.
    It initializes the necessary configurations, sets up the input data, executes the
    spatial aggregation process, and asserts that the output DataFrame matches the
    expected result.

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialize configuration paths.
    2. Create input data by setting up grid zone map data and population test data.
    3. Initialize the SpatialAggregation component with the general and specific configuration paths.
    4. Execute the spatial aggregation process.
    5. Read the output DataFrame from the spatial aggregation component.
    6. Retrieve the expected output DataFrame using the predefined schema and path.
    7. Assert that the output DataFrame is equal to the expected output DataFrame.
    """
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/spatial_aggregation/spatial_aggregation_population.ini"
    ## Create Input data
    set_grid_zone_map_data(spark)
    set_population_test_data(spark)

    ## Init component class
    spatial_aggregation = SpatialAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    spatial_aggregation.execute()

    # Assertion
    # read from test data output
    output_data_object = spatial_aggregation.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_data_object.read()

    # assert read data == expected
    expected_result = generate_expected_population_zone_data("2023-01-01T00:00:00")
    expected_result = spark.createDataFrame(expected_result, schema=SilverPresentPopulationZoneDataObject.SCHEMA)

    assertDataFrameEqual(output_data_object.df, expected_result)


def test_ue_spatial_aggregation(spark):
    """
    Test the spatial aggregation of usual environments (UE) data.

    DESCRIPTION:
    This test verifies the spatial aggregation component for usual environments data.
    It initializes the necessary configurations, sets up the input data, executes the
    spatial aggregation process, and asserts that the output DataFrame matches the
    expected result.

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialize configuration paths.
    2. Create input data by setting up grid zone map data and usual environments test data.
    3. Initialize the SpatialAggregation component with the general and specific configuration paths.
    4. Execute the spatial aggregation process.
    5. Read the output DataFrame from the spatial aggregation component.
    6. Retrieve the expected output DataFrame using the predefined schema and path.
    7. Assert that the output DataFrame is equal to the expected output DataFrame.
    """
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/spatial_aggregation/spatial_aggregation_ue.ini"
    ## Create Input data
    set_grid_zone_map_data(spark)
    set_ue_test_data(spark)

    ## Init component class
    spatial_aggregation = SpatialAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    spatial_aggregation.execute()

    # Assertion
    # read from test data output
    output_data_object = spatial_aggregation.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_data_object.read()

    # assert read data == expected
    expected_result = generate_expected_ue_zone_data("2023-01", "2023-03")
    expected_result = spark.createDataFrame(
        expected_result, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
    )

    assertDataFrameEqual(output_data_object.df, expected_result)


def test_present_population_spatial_aggregation_1km_grid(spark):
    """
    Test the spatial aggregation of present population data to 1km INSPIRE grid as zoning dataset.

    DESCRIPTION:
    This test verifies the spatial aggregation component for present population data.
    It initializes the necessary configurations, sets up the input data, executes the
    spatial aggregation process, and asserts that the output DataFrame matches the
    expected result.

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialize configuration paths.
    2. Create input data by setting up population test data.
    3. Initialize the SpatialAggregation component with the general and specific configuration paths.
    4. Execute the spatial aggregation process.
    5. Read the output DataFrame from the spatial aggregation component.
    6. Retrieve the expected output DataFrame using the predefined schema and path.
    7. Assert that the output DataFrame is equal to the expected output DataFrame.
    """
    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/spatial_aggregation/spatial_aggregation_population_1km_grid.ini"
    )
    ## Create Input data
    set_population_test_data(spark)

    ## Init component class
    spatial_aggregation = SpatialAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    spatial_aggregation.execute()

    # Assertion
    # read from test data output
    output_data_object = spatial_aggregation.output_data_objects[SilverPresentPopulationZoneDataObject.ID]
    output_data_object.read()

    # assert read data == expected
    expected_result = generate_expected_population_1km_grid_data("2023-01-01T00:00:00")
    expected_result = spark.createDataFrame(expected_result, schema=SilverPresentPopulationZoneDataObject.SCHEMA)

    assertDataFrameEqual(output_data_object.df, expected_result)


def test_ue_spatial_aggregation_1km_grid(spark):
    """
    Test the spatial aggregation of usual environments (UE) data to 1km INSPIRE grid as zoning dataset.

    DESCRIPTION:
    This test verifies the spatial aggregation component for usual environments data.
    It initializes the necessary configurations, sets up the input data, executes the
    spatial aggregation process, and asserts that the output DataFrame matches the
    expected result.

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialize configuration paths.
    2. Create input data by setting up usual environments test data.
    3. Initialize the SpatialAggregation component with the general and specific configuration paths.
    4. Execute the spatial aggregation process.
    5. Read the output DataFrame from the spatial aggregation component.
    6. Retrieve the expected output DataFrame using the predefined schema and path.
    7. Assert that the output DataFrame is equal to the expected output DataFrame.
    """
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/spatial_aggregation/spatial_aggregation_ue_1km_grid.ini"
    ## Create Input data
    set_ue_test_data(spark)

    ## Init component class
    spatial_aggregation = SpatialAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    spatial_aggregation.execute()

    # Assertion
    # read from test data output
    output_data_object = spatial_aggregation.output_data_objects[SilverAggregatedUsualEnvironmentsZonesDataObject.ID]
    output_data_object.read()

    # assert read data == expected
    expected_result = generate_expected_ue_1km_grid_data("2023-01", "2023-03")
    expected_result = spark.createDataFrame(
        expected_result, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
    )

    assertDataFrameEqual(output_data_object.df, expected_result)
