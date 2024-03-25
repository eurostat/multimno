from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.components.execution.cell_footprint.cell_footprint_estimation import CellFootprintEstimation

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.cell_footprint.aux_cell_footprint_testing import (
    expected_footprint,
    expected_intersection_groups,
    set_input_data,
)
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_footprint, expected_intersection_groups]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_cell_footprint_estimation(spark, expected_footprint, expected_intersection_groups):
    """
    DESCRIPTION:
        Test shall execute the CellFootprintEstimation component with a signal stremgth dataframe containing
        4 rows with signal strength values in single grid tile for 4 nerby cells. 1 cell signal strentgh is
        below the threshold and 3 are above. The expected outputs:
        1. Cell Footprint data frame with 3 rows of signal dominance values for cells which were not filtered by
        prunning.
        2. Cell Intersection Groups data frame with 4 rows of all possible cell intersection groups
        for the given grid tile.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/network/cell_footprint_estimation/cell_footprint_estimation.ini
        Input Data:
            silver_signal_strength: /opt/testing_data/lakehouse/silver/signal_strength

    OUTPUT:
        cell_footprint_data_silver:  /opt/testing_data/lakehouse/cell_footprint
        cell_intersection_groups_data_silver:  /opt/testing_data/lakehouse/cell_intersection_groups

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the CellFootprintEstimation component with the test configs
        4.- Execute the CellFootprintEstimation (includes read, transform, write)
        5.- Read written Cell Footprint data object with SignalStrengthModeling class.
        6.- Read written Cell Intesection Groups data object with SignalStrengthModeling class.
        7.- Assert DataFrames are equal.
    """
    # Setup

    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/network/cell_footprint_estimation/cell_footprint_estimation.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(spark, config)

    ## Init component class
    test_component = CellFootprintEstimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected (defined as fixture)

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_footprint_data_object = test_component.output_data_objects[SilverCellFootprintDataObject.ID]
    output_footprint_data_object.read()

    output_intersection_groups_data_object = test_component.output_data_objects[
        SilverCellIntersectionGroupsDataObject.ID
    ]
    output_intersection_groups_data_object.read()

    # assert read data == expected
    assertDataFrameEqual(expected_footprint, output_footprint_data_object.df)
    assertDataFrameEqual(expected_intersection_groups, output_intersection_groups_data_object.df)
