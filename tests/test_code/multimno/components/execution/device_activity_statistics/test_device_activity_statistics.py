from pyspark.testing.utils import assertDataFrameEqual
from multimno.core.configuration import parse_configuration

from multimno.core.constants.columns import ColNames
from multimno.components.execution.device_activity_statistics.device_activity_statistics import DeviceActivityStatistics
from multimno.core.data_objects.silver.silver_device_activity_statistics import SilverDeviceActivityStatistics
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir
from tests.test_code.multimno.components.execution.device_activity_statistics.aux_device_activity_statistics import (
    write_input_data,
    expected_device_activity_statistics,
)


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_device_activity_statistics]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_device_activity_statistics(spark, expected_device_activity_statistics):
    # Setup
    # define config path
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/device_activity_statistics/testing_device_activity_statistics.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    write_input_data(spark, config)

    # Init component class
    device_activity_statistics_component = DeviceActivityStatistics(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    device_activity_statistics_component.execute()

    # Read results
    device_activity_statistics_component.output_data_objects[SilverDeviceActivityStatistics.ID].read()
    result_df = device_activity_statistics_component.output_data_objects[SilverDeviceActivityStatistics.ID].df
    # Reorder columns to match Schema
    result_df = result_df.select([col.name for col in SilverDeviceActivityStatistics.SCHEMA])

    expected_device_activity_statistics.show()
    result_df.show()

    # Assertion
    assertDataFrameEqual(result_df, expected_device_activity_statistics)
