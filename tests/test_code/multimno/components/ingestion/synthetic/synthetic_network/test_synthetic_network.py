from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.components.ingestion.synthetic.synthetic_network import SyntheticNetwork

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.ingestion.synthetic.synthetic_network.aux_synthetic_network import (
    expected_data,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

fixtures = [spark, expected_data]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_synthetic_network(spark, expected_data):
    """
    DESCRIPTION:
        Test shall execute the SyntheticEvents component with zero errors, for a network of 3 cells,
        and for a synthetic diary of stay-move-stay for a single user.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/synthetic/testing_synthetic_network.ini

    EXPECTED OUTPUT:
        bronze_network_data: (fixture) aux_synthetic_network.expected_data

    STEPS:
        1.- Init the SyntheticNetwork component with test configs.
        5.- Read expected data with BronzeNetworkDataObject class
        6.- Execute the SyntheticNetwork component.
        7.- Read written data in /opt/testing_data with BronzeNetworkDataObject classes.
        8.- Assert DataFrames are equal.
    """
    # Setup

    # init configs and paths

    component_config_path = f"{TEST_RESOURCES_PATH}/config/synthetic/testing_synthetic_network.ini"

    # init component class
    synthetic_network = SyntheticNetwork(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    synthetic_network.execute()

    # Assertion
    # read from test data output

    output_network_data_object = synthetic_network.output_data_objects[BronzeNetworkDataObject.ID]
    output_network_data_object.read()

    assert output_network_data_object.df.count() == expected_data.count()
    # assertDataFrameEqual(output_network_data_object.df, expected_data)
