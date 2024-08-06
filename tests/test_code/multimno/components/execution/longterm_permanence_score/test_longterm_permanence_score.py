import pytest
from pyspark.testing.utils import assertDataFrameEqual

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir
from tests.test_code.multimno.components.execution.longterm_permanence_score.aux_longterm_permanence_score import (
    data_test_0001,
    set_input_data,
    input_midterm_permanence_score_id,
    expected_output_longterm_permenence_score_id,
    get_expected_output_df,
)
from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)
from multimno.components.execution.longterm_permanence_score.longterm_permanence_score import LongtermPermanenceScore
from multimno.core.constants.columns import ColNames


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


@pytest.mark.parametrize("get_test_data", [data_test_0001])
def test_longterm_permanence_score(spark, get_test_data):
    """
    DESCRIPTION:
        Test shall execute the LongtermPermanenceScore component.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/longterm_permanence_score/testing_longtime_permanence_score.ini

    STEPS:
        1.- Init the LongtermPermanenceScore component with test configs.
        2.- Write provided input data to expcted path locations.
        3.- Execute the LongtermPermanenceScore.
        4.- Read written data with SilverMidtermPermanenceScoreDataObject class.
        5.- Assert read DataFrame is equal to provided expected output DataFrame.
    """
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/longterm_permanence_score/testing_longterm_permanence_score.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_midterm_permanence_score_id],
    )

    ## Init component class
    longterm_permanence_score = LongtermPermanenceScore(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    longterm_permanence_score.execute()

    # Assertion
    # Prepare output of test execution
    output_data_object = longterm_permanence_score.output_data_objects[SilverLongtermPermanenceScoreDataObject.ID]
    output_data_object.read()
    orderByCols = [ColNames.user_id, ColNames.season, ColNames.day_type, ColNames.time_interval, ColNames.start_date]
    output_df = output_data_object.df.orderBy(orderByCols)
    # Prepare expected output
    expected_output = get_expected_output_df(spark, test_data_dict[expected_output_longterm_permenence_score_id])
    expected_output = expected_output.orderBy(orderByCols)
    # Reorder columns to match schema. (Why are they out of order anyway?)
    output_df = output_df.select(SilverLongtermPermanenceScoreDataObject.SCHEMA.fieldNames())
    # assert data equality
    assertDataFrameEqual(output_df, expected_output)
