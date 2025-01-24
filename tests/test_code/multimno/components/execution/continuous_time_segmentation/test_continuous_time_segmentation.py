import pytest
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_time_segments_data_object import (
    SilverTimeSegmentsDataObject,
)
from multimno.components.execution.time_segments.continuous_time_segmentation import (
    ContinuousTimeSegmentation,
)
from multimno.core.constants.columns import ColNames

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.continuous_time_segmentation.aux_continuous_time_segmentation import (
    set_input_data,
    get_expected_output_df,
)
from tests.test_code.multimno.components.execution.continuous_time_segmentation.aux_continuous_time_segmentation import (
    input_events_id,
    input_cell_intersection_groups_id,
    expected_output_time_segments_id,
)
from tests.test_code.multimno.components.execution.continuous_time_segmentation.aux_continuous_time_segmentation import (
    data_test_0001,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


@pytest.mark.parametrize("get_test_data", [data_test_0001])
def test_continuous_time_segmentation(spark, get_test_data):
    """
    DESCRIPTION:
        Test shall execute the ContinuousTimeSegmentation component.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/time_segments/testing_time_segments.ini

    STEPS:
        1.- Init the ContinuousTimeSegmentation component with test configs.
        2.- Write provided input data to expcted path locations.
        3.- Execute the ContinuousTimeSegmentation.
        4.- Read written data with SilverTimeSegmentsDataObject class.
        5.- Assert read DataFrame is equal to provided expected output DataFrame.
    """
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/time_segments/testing_time_segments.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_events_id],
        test_data_dict[input_cell_intersection_groups_id],
    )

    ## Init component class
    continuous_time_segmentation = ContinuousTimeSegmentation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    continuous_time_segmentation.execute()

    # Assertion
    # read from test data output
    output_data_object = continuous_time_segmentation.output_data_objects[SilverTimeSegmentsDataObject.ID]
    output_data_object.read()

    output_df = output_data_object.df.orderBy([ColNames.user_id, ColNames.start_timestamp])
    # assert read data == expected
    expected_time_segments = get_expected_output_df(spark, test_data_dict[expected_output_time_segments_id])

    # May need to explicitly sort "cells" for equality checks
    assertDataFrameEqual(output_df, expected_time_segments)
