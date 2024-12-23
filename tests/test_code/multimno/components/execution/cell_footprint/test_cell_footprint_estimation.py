from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)
from multimno.components.execution.cell_footprint.cell_footprint_estimation import (
    CellFootprintEstimation,
)
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.cell_footprint.aux_cell_footprint_testing import (
    EXPECTED_IMPUTE_DEFAULT_PROPERTIES,
    get_mno_network,
    set_input_network_data,
)
from tests.test_code.test_common import (
    TEST_RESOURCES_PATH,
    TEST_GENERAL_CONFIG_PATH,
    STATIC_TEST_DATA_PATH,
)
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]

CELLFOOTPRINT_CONFIG_PATH = (
    f"{TEST_RESOURCES_PATH}/config/network/cell_footprint_estimation/cell_footprint_estimation.ini"
)


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

    grid_do = SilverGridDataObject(spark, config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver"), ["quadkey"])

    grid_sdf = (
        spark.read.format("geoparquet")
        .schema(SilverGridDataObject.SCHEMA)
        .load(f"{STATIC_TEST_DATA_PATH}/grid/expected_extent_grid")
    )
    grid_do.df = grid_sdf
    grid_do.write()

    set_input_network_data(spark, config)


def test_cell_footprint_estimation(spark):
    """
    DESCRIPTION:
        Test shall execute the CellFootprintEstimation component with a genereated cells data
        and pregenerated enriched grid data. The test shall assert the output cell footprint data
        is equal to the pregenerated expected cell footrpint data.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/network/cell_footprint_estimation/cell_footprint_estimation.ini
        Input Data:
            silver_signal_strength: /opt/testing_data/lakehouse/silver/signal_strength

    OUTPUT:
        cell_footprint_data_silver:  /opt/tests/test_resources/test_data/network/cell_footprint
    """
    # Setup

    ## Init configs & paths

    ## Create Input data
    prepare_test_data(spark)

    ## Init component class
    test_component = CellFootprintEstimation(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)

    # Expected
    expected_do = SilverCellFootprintDataObject(spark, f"{STATIC_TEST_DATA_PATH}/network/cell_footprint/")
    expected_do.read()
    expected_sdf = expected_do.df
    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_footprint_data_object = test_component.output_data_objects[SilverCellFootprintDataObject.ID]
    output_footprint_data_object.read()
    # assert read data == expected
    assertDataFrameEqual(output_footprint_data_object.df, expected_sdf)


def test_impute_default_cell_properties(spark):
    """
    DESCRIPTION:
        Test the impute_default_cell_properties method of CellFootprintEstimation class.

    INPUT:
        sdf: Input DataFrame with null values.

    OUTPUT:
        sdf_imputed: DataFrame with imputed default cell properties.
    """
    prepare_test_data(spark)
    # Create test input DataFrame
    sdf = get_mno_network(spark)

    # Create instance of CellFootprintEstimation class
    component = CellFootprintEstimation(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)

    # Call the impute_default_cell_properties method
    sdf_imputed = component.impute_default_cell_properties(sdf)

    # Define expected output DataFrame
    expected_sdf = spark.createDataFrame(EXPECTED_IMPUTE_DEFAULT_PROPERTIES, schema=sdf_imputed.schema)

    # Assert that the output DataFrame matches the expected DataFrame
    assertDataFrameEqual(sdf_imputed, expected_sdf)
