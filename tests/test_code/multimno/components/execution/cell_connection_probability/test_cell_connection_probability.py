from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from pyspark.testing.utils import assertDataFrameEqual

from multimno.components.execution.cell_connection_probability.cell_connection_probability import (
    CellConnectionProbabilityEstimation,
)

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.cell_connection_probability.aux_cell_connection_probability import (
    expected_cell_connection_probabilities,
    set_input_cell_footprint_data,
    set_input_grid_data,
)

from multimno.core.configuration import parse_configuration

# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_cell_connection_probabilities]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_cell_connection_probability(spark, expected_cell_connection_probabilities):
    """
    DESCRIPTION:
        Test shall execute the CellConnectionProbabilityEstimation component with a
        cell footprint input data of 3 rows and grid data of 2 rows.
        The expected values for the posterior probability, given that use_land_use_prior = True,
        are defined in the test.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/cell_connection_probability/cell_connection_probability.ini
        Input Data:
            cell_footprint_data_silver = ${Paths:silver_dir}/cell_footprint
            grid_data_silver: grid_data_silver = ${Paths:silver_dir}/grid

    EXPECTED OUTPUT:
        cell_connection_probabilities_data_silver: = ${Paths:silver_dir}/cell_connection_probabilities_data_silver

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the CellConnectionProbabilityEstimation component with the test configs
        4.- Execute the CellConnectionProbabilityEstimation (includes read, transform, write)
        5.- Read written data in /opt/testing_data with SilverCellConnectionProbabilities class.
        6.- Assert DataFrames are equal.
    """

    # Setup
    # define config path
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/cell_connection_probability/" "testing_cell_connection_probability.ini"
    )

    # Generate input
    parsed_config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    set_input_cell_footprint_data(spark, parsed_config)
    set_input_grid_data(spark, parsed_config)

    # Init component class
    cell_connection_prob_module = CellConnectionProbabilityEstimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    cell_connection_prob_module.execute()

    # Assertion
    # read from test data output
    generated_output = cell_connection_prob_module.output_data_objects[
        SilverCellConnectionProbabilitiesDataObject.ID
    ].df
    assertDataFrameEqual(generated_output, expected_cell_connection_probabilities)
