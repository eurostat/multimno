from multimno.components.execution.grid_enrichment.grid_enrichment import GridEnrichment
from multimno.core.data_objects.bronze.bronze_transportation_data_object import (
    BronzeTransportationDataObject,
)
from multimno.core.data_objects.bronze.bronze_landuse_data_object import (
    BronzeLanduseDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
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


def prepare_test_data(spark):
    """
    DESCRIPTION:
        Function to prepare the test data for the tests.
    """
    # Prepare test data
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)

    transportation_do = BronzeTransportationDataObject(
        spark,
        config.get(CONFIG_BRONZE_PATHS_KEY, "transportation_data_bronze"),
    )

    transportation_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/spatial_data/transportation")

    transportation_do.df = transportation_sdf
    transportation_do.write()

    landuse_do = BronzeLanduseDataObject(spark, config.get(CONFIG_BRONZE_PATHS_KEY, "landuse_data_bronze"))

    landuse_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/spatial_data/landuse")

    landuse_do.df = landuse_sdf
    landuse_do.write()

    grid_do = SilverGridDataObject(spark, config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver"), ["quadkey"])

    grid_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/grid/expected_extent_grid")

    grid_do.df = grid_sdf
    grid_do.write()


def test_inspire_grid_enrichment(spark):
    """
    DESCRIPTION:
        This test executes the GridEnrichment component. The expected output is a grid with
        landuse prior and PLE values of SilverEnrichedGridDataObject schema.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/grid/grid_enrichment.ini
        Input Data: Prepared by the prepare_test_data function

    OUTPUT:
        grid_data_silver:  testing_data/lakehouse/grid_enriched

    STEPS:
        1.- Prepare the test data
        2.- Parse the configuration
        3.- Initialize the GridEnrichment component with the test configs
        4.- Execute the GridEnrichment component (includes read, transform, write)
        5.- Read the written Grid data object with the GridEnrichment class
        6.- Assert that the output DataFrame is equal to the expected DataFrame using the assert_sparkgeodataframe_equal utility function
    """
    # Setup
    prepare_test_data(spark)
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/grid/grid_enrichment.ini"

    ## Init component class
    test_component = GridEnrichment(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected
    expected_do = SilverEnrichedGridDataObject(spark, f"{STATIC_TEST_DATA_PATH}/grid/expected_grid_enriched")
    expected_do.read()
    expected_sdf = expected_do.df

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_grid_data_object = test_component.output_data_objects[SilverEnrichedGridDataObject.ID]
    output_grid_data_object.read()

    # assert read data == expected
    assert_sparkgeodataframe_equal(output_grid_data_object.df, expected_sdf)
