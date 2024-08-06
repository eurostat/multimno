from pyspark.testing.utils import assertDataFrameEqual

from multimno.components.execution.geozones_grid_mapping.geozones_grid_mapping import (
    GeozonesGridMapping,
)
from multimno.core.data_objects.bronze.bronze_geographic_zones_data_object import (
    BronzeGeographicZonesDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
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

    zones_do = BronzeGeographicZonesDataObject(
        spark,
        config.get(CONFIG_BRONZE_PATHS_KEY, "geographic_zones_data_bronze"),
    )

    zones_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/spatial_data/nuts_zones")

    zones_do.df = zones_sdf
    zones_do.write()

    grid_do = SilverGridDataObject(spark, config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver"))

    grid_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/grid/expected_extent_grid")

    grid_do.df = grid_sdf
    grid_do.write()


def test_geozones_grid_mapping(spark):
    """
    DESCRIPTION:
        This test executes the GeozonesGridMapping component. The expected output is a grid of a test extent
        with mapped NUTS zones IDs.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/grid/geozones_grid_mapping.ini
        Input Data: Prepared by the prepare_test_data function

    OUTPUT:
        grid_data_silver:  testing_data/lakehouse/silver/geozones_grid_map_data_silver

    STEPS:
        1.- Prepare the test data
        2.- Parse the configuration
        3.- Initialize the GeozonesGridMapping component with the test configs
        4.- Execute the GeozonesGridMapping component
        5.- Read the written Grid data object with the GeozonesGridMapping class
        6.- Assert that the output DataFrame is equal to the expected DataFrame using the assertDataFrameEqual utility function
    """
    # Setup
    prepare_test_data(spark)
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/grid/geozones_grid_mapping.ini"

    ## Init component class
    test_component = GeozonesGridMapping(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected
    expected_do = SilverGeozonesGridMapDataObject(spark, f"{STATIC_TEST_DATA_PATH}/grid/expected_geozones_grid_map")
    expected_do.read()
    expected_sdf = expected_do.df

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_grid_data_object = test_component.output_data_objects[SilverGeozonesGridMapDataObject.ID]
    output_grid_data_object.read()

    # assert read data == expected
    assertDataFrameEqual(output_grid_data_object.df, expected_sdf)
