import pytest
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.utils import apply_schema_casting
from multimno.core.configuration import parse_configuration
from multimno.components.execution.present_population.present_population_estimation import (
    PresentPopulationEstimation,
)

from multimno.core.constants.columns import ColNames

from tests.test_code.fixtures import spark_session as spark
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_present_population_data_object import (
    SilverPresentPopulationDataObject,
)

from tests.test_code.multimno.components.execution.present_population_estimation.aux_present_population_estimation import (
    set_input_data,
    input_events_id,
    input_grid_id,
    input_cell_connection_probability_id,
    expected_present_population_id,
    get_expected_output_df,
    data_test_grid_0001,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


@pytest.mark.parametrize("get_test_data", [data_test_grid_0001])
def test_present_population_grid(spark, get_test_data):
    """
    DESCRIPTION:
        Test shall execute the PresentPopulationEstimation component, testing grid-aggregated results.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/present_population/testing_present_population.ini

    STEPS:
        1.- Init the PresentPopulationEstimation component with test configs.
        2.- Write provided input data to expcted path locations.
        3.- Execute the PresentPopulationEstimation.
        4.- Read written data with SilverPresentPopulationDataObject class.
        5.- Assert read DataFrame is equal to provided expected output DataFrame.
    """
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/present_population/testing_present_population.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_events_id],
        test_data_dict[input_grid_id],
        test_data_dict[input_cell_connection_probability_id],
    )

    ## Init component class
    present_population_estimation = PresentPopulationEstimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    present_population_estimation.execute()

    # Assertion
    # read from test data output
    output_data_object = present_population_estimation.output_data_objects[SilverPresentPopulationDataObject.ID]
    output_data_object.read()

    output_df = output_data_object.df.orderBy([ColNames.timestamp, ColNames.grid_id])
    output_df = apply_schema_casting(output_df, SilverPresentPopulationDataObject.SCHEMA)
    # assert read data == expected

    expected_result = get_expected_output_df(
        spark,
        test_data_dict[expected_present_population_id],
        schema=SilverPresentPopulationDataObject.SCHEMA,
    )

    # TODO debug remove
    output_df.show()
    expected_result.show()

    assertDataFrameEqual(output_df, expected_result)
