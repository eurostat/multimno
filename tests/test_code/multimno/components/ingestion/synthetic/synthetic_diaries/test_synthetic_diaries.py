from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.bronze.bronze_synthetic_diaries_data_object import BronzeSyntheticDiariesDataObject
from multimno.components.ingestion.synthetic.synthetic_diaries import SyntheticDiaries

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.ingestion.synthetic.synthetic_diaries.aux_synthetic_diaries import (
    expected_data,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

fixtures = [spark, expected_data]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_synthetic_diaries(spark, expected_data):
    """
    DESCRIPTION:
        Test shall execute the SyntheticDiaries component.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/synthetic/testing_synthetic_diaries.ini

    EXPECTED OUTPUT:
        bronze_diaries_data: (fixture) aux_synthetic_diaries.expected_data

    STEPS:
        1.- Init the SyntheticDiaries component with test configs.
        2.- Read expected data with BronzeSyntheticDiariesDataObject class
        3.- Execute the SyntheticDiaries component.
        4.- Read written data in /opt/testing_data with BronzeSyntheticDiariesDataObject classes.
        5.- Assert DataFrames are equal.
    """
    # Setup
    # init configs and paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/synthetic/testing_synthetic_diaries.ini"

    # init component class
    synthetic_diaries = SyntheticDiaries(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    synthetic_diaries.execute()

    # Assertion
    # read from test data output
    output_network_data_object = synthetic_diaries.output_data_objects[BronzeSyntheticDiariesDataObject.ID]
    output_network_data_object.read()

    assertDataFrameEqual(output_network_data_object.df, expected_data)
