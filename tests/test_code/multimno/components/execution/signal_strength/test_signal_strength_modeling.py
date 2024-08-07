from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_signal_strength_data_object import (
    SilverSignalStrengthDataObject,
)
from multimno.components.execution.signal_strength.signal_stength_modeling import (
    SignalStrengthModeling,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.signal_strength.aux_signal_strength_testing import (
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

    grid_sdf = spark.read.format("geoparquet").load(f"{STATIC_TEST_DATA_PATH}/grid/expected_extent_grid")

    grid_do.df = grid_sdf
    grid_do.write()

    set_input_network_data(spark, config)


def test_signal_strength_modeling(spark):
    """
    DESCRIPTION:
        Test shall execute the SignalStrengthModeling component with a cell dataframe of two rows representing
        1 directional and 1 omnidirectional cells. Additionally, a grid dataframe with set of grids covering the diameter
        of both cells range. The expected output is a dataframe with the signal strength of each grid point for each cell.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/network/signal_strength_modeling/signal_strength_modeling.ini
        Input Data:
            silver_network: /opt/testing_data/lakehouse/silver/mno_network
            silver_grid: /opt/testing_data/lakehouse/silver/grid

    OUTPUT:
        signal_strength_data_silver:  /opt/testing_data/lakehouse/signal_strength_data_silver

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the SignalStrengthModeling component with the test configs
        4.- Execute the SignalStrengthModeling (includes read, transform, write)
        5.- Read written Signal Strength data object with SignalStrengthModeling class.
        6.- Assert DataFrames are equal.
    """
    # Setup

    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/network/signal_strength_modeling/signal_strength_modeling.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    prepare_test_data(spark)

    ## Init component class
    signal_strength_modeling = SignalStrengthModeling(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected
    expected_do = SilverSignalStrengthDataObject(spark, f"{STATIC_TEST_DATA_PATH}/network/expected_signal_strength")
    expected_do.read()
    expected_sdf = expected_do.df

    # Execution
    signal_strength_modeling.execute()

    # Assertion
    # read from test data output
    output_data_object = signal_strength_modeling.output_data_objects[SilverSignalStrengthDataObject.ID]
    output_data_object.read()
    # assert read data == expected
    assertDataFrameEqual(expected_sdf, output_data_object.df)
