from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.components.execution.daily_permanence_score.daily_permanence_score import DailyPermanenceScore
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.daily_permanence_score.aux_dps_testing import (
    set_input_data,
    expected_data,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_data]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_daily_permanence_score(spark, expected_data):
    """
    DESCRIPTION:
        Test shall execute the DailyPermanenceScore component. The expected output is a dataframe
        with the DPS for each user, time slot and grid tile.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/daily_aggregations/daily_permanence_score.ini
        Input Data:
            event_data_silver_flagged: /opt/testing_data/lakehouse/silver/mno_events_flagged
            cell_footprint_data_silver: /opt/testing_data/lakehouse/silver/cell_footprint

    OUTPUT:
        daily_permanence_score_data_silver:  /opt/testing_data/lakehouse/silver/daily_permanence_score

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiliary file
        3.- Init the DailyPermanenceScore component with the test configs
        4.- Execute the DailyPermanenceScore (includes read, transform, write)
        5.- Read resulting SilverDailyPermanenceScoreDataObject.
        6.- Load expected SilverDailyPermanenceScoreDataObject.
        7.- Assert DataFrames are equal.
    """
    # Setup

    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/daily_aggregations/daily_permanence_score.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data(spark, config)

    ## Init component class
    dps_component = DailyPermanenceScore(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected (defined as fixture)

    # Execution
    dps_component.execute()

    # Assertion
    # read from test data output
    output_data_object = dps_component.output_data_objects[SilverDailyPermanenceScoreDataObject.ID]
    output_data_object.read()

    # print(output_data_object.df.show(50))
    # print(expected_data.show(50))

    # assert read data == expected
    assertDataFrameEqual(output_data_object.df, expected_data)
