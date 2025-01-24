import pytest
from multimno.components.execution.tourism_stays_estimation.tourism_stays_estimation import TourismStaysEstimation
from multimno.core.data_objects.silver.silver_tourism_stays_data_object import SilverTourismStaysDataObject
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.tourism_stays_estimation.aux_tourism_stays_estimation import (
    set_input_data,
    get_expected_output_df,
)
from tests.test_code.multimno.components.execution.tourism_stays_estimation.aux_tourism_stays_estimation import (
    input_time_segments_id,
    input_cell_connection_probabilities_id,
    input_geozones_grid_mapping_id,
    expected_tourism_stays_id,
    input_ue_labels_id,
)
from tests.test_code.multimno.components.execution.tourism_stays_estimation.aux_tourism_stays_estimation import (
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
def test_tourism_stays_estimation(spark, get_test_data):
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/tourism_stays_estimation/testing_tourism_stays_estimation.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_time_segments_id],
        test_data_dict[input_cell_connection_probabilities_id],
        test_data_dict[input_geozones_grid_mapping_id],
        test_data_dict[input_ue_labels_id],
    )

    ## Init component class
    tourism_stays_estimation = TourismStaysEstimation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    tourism_stays_estimation.execute()

    # Assertion
    # read from test data output
    output_data_object = tourism_stays_estimation.output_data_objects[SilverTourismStaysDataObject.ID]
    output_data_object.read()

    output_df = output_data_object.df.orderBy([ColNames.user_id, ColNames.start_timestamp])
    # assert read data == expected
    expected_tourism_stays = get_expected_output_df(spark, test_data_dict[expected_tourism_stays_id])

    # May need to explicitly sort "cells" for equality checks
    assertDataFrameEqual(output_df, expected_tourism_stays)
