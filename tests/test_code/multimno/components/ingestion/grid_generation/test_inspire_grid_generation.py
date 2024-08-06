from multimno.components.ingestion.grid_generation.inspire_grid_generation import (
    InspireGridGeneration,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.bronze.bronze_countries_data_object import (
    BronzeCountriesDataObject,
)

from tests.test_code.fixtures import spark_session as spark
from multimno.core.configuration import parse_configuration
from tests.test_code.test_common import (
    TEST_RESOURCES_PATH,
    TEST_GENERAL_CONFIG_PATH,
    STATIC_TEST_DATA_PATH,
)
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
    CONFIG_BRONZE_PATHS_KEY,
    CONFIG_PATHS_KEY,
)
from tests.test_code.test_utils import (
    setup_test_data_dir,
    teardown_test_data_dir,
    assert_sparkgeodataframe_equal,
)


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def prepare_test_countries_data(spark):
    """
    DESCRIPTION:
        Function to prepare the test data for the tests.
    """
    # Prepare test data
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)

    countries_do = BronzeCountriesDataObject(
        spark,
        config.get(CONFIG_BRONZE_PATHS_KEY, "countries_data_bronze"),
    )

    countries_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/spatial_data/countries")

    countries_do.df = countries_sdf
    countries_do.write()


def test_inspire_grid_generation_extent(spark):
    """
    DESCRIPTION:
        Test shall execute the InspireGridGeneration component with a given extent. The expected otuput is
        an INSPIRE grid of 100m x 100m tiles covering the extent with tile centroids geometry.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/config/grid/grid_generation.ini
        Input Data:

    OUTPUT:
        grid_data_silver:  testing_data/lakehouse/silver/grid

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the InspireGridGeneration component with the test configs
        4.- Execute the InspireGridGeneration (includes read, transform, write)
        5.- Read written Grid data object with InspireGridGeneration class.
        7.- Assert DataFrames are equal using geodataframe comparions utility function.
    """
    # Setup

    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/grid/grid_generation_extent.ini"

    ## Init component class
    test_component = InspireGridGeneration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected
    expected_do = SilverGridDataObject(spark, f"{STATIC_TEST_DATA_PATH}/grid/expected_extent_grid")
    expected_do.read()
    expected_sdf = expected_do.df

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_grid_data_object = test_component.output_data_objects[SilverGridDataObject.ID]
    output_grid_data_object.read()

    # assert read data == expected
    assert_sparkgeodataframe_equal(output_grid_data_object.df, expected_sdf)


def test_inspire_grid_generation_polygon(spark):
    """
    DESCRIPTION:
        Test shall execute the InspireGridGeneration component with a given country polygon. The expected otuput is
        an INSPIRE grid of 100m x 100m tiles covering the polygon with tile centroids geometry.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/config/grid/grid_generation.ini
        Input Data:

    OUTPUT:
        grid_data_silver:  testing_data/lakehouse/silver/grid

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the InspireGridGeneration component with the test configs
        4.- Execute the InspireGridGeneration (includes read, transform, write)
        5.- Read written Grid data object with InspireGridGeneration class.
        7.- Assert DataFrames are equal using geodataframe comparions utility function.
    """
    # Setup
    prepare_test_countries_data(spark)
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/grid/grid_generation_polygon.ini"

    ## Init component class
    test_component = InspireGridGeneration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected
    expected_do = SilverGridDataObject(spark, f"{STATIC_TEST_DATA_PATH}/grid/expected_polygon_grid")
    expected_do.read()
    expected_sdf = expected_do.df

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_grid_data_object = test_component.output_data_objects[SilverGridDataObject.ID]
    output_grid_data_object.read()

    # assert read data == expected
    assert_sparkgeodataframe_equal(output_grid_data_object.df, expected_sdf)
