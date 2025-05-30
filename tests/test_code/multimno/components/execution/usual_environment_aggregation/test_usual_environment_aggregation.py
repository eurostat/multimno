import pytest
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration

from multimno.components.execution.usual_environment_aggregation.usual_environment_aggregation import (
    UsualEnvironmentAggregation,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.usual_environment_aggregation.aux_ue_aggregation_testing import (
    set_input_data,
    set_input_data_uniform,
    expected_data,
    expected_data_uniform,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark, expected_data, expected_data_uniform]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


# @pytest.mark.skip(reason="TODO: When enriched grid is redesigned")
# def test_usual_environment_aggregation(spark, expected_data):
#     """
#     DESCRIPTION:
#         Test shall execute the UsualEnvironmentAggregation component. The expected output is a dataframe
#         with the total aggregated usual environments, homes and works in each grid tile.

#     INPUT:
#         Test Configs:
#             general: tests/test_resources/config/general_config.ini
#             component: tests/test_resources/config/usual_environment_aggregation/usual_environment_aggregation.ini
#         Input Data:
#             enriched_grid_data_silver: /opt/testing_data/lakehouse/silver/grid_enriched
#             usual_environment_labels_data_silver: /opt/testing_data/lakehouse/silver/usual_environment_labels

#     OUTPUT:
#         aggregated_usual_environments_silver:  /opt/testing_data/lakehouse/silver/aggregated_usual_environments

#     STEPS:
#         1.- Parse the configuration
#         2.- Generate the input data using functions from the auxiliary file
#         3.- Init the UsualEnvironmentAggregation component with the test configs
#         4.- Execute the UsualEnvironmentAggregation (includes read, transform, write)
#         5.- Read resulting SilverAggregatedUsualEnvironmentsDataObject.
#         6.- Load expected SilverAggregatedUsualEnvironmentsDataObject.
#         7.- Assert DataFrames are equal.
#     """
#     # ========================================================================================================
#     # TEST TAKING INTO ACCOUNT VARIABLE GRID TILE WEIGHTS
#     # ========================================================================================================

#     # Setup

#     ## Init configs & paths
#     component_config_path = (
#         f"{TEST_RESOURCES_PATH}/config/usual_environment_aggregation/usual_environment_aggregation.ini"
#     )
#     config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

#     ## Create Input data
#     set_input_data(spark, config)

#     ## Init component class
#     component = UsualEnvironmentAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

#     # Expected (defined as fixture)

#     # Execution
#     component.execute()

#     # Assertion
#     # read from test data output
#     output_data_object = component.output_data_objects[SilverAggregatedUsualEnvironmentsDataObject.ID]
#     output_data_object.read()

#     # print(output_data_object.df.show(50))
#     # print(expected_data.show(50))

#     # assert read data == expected
#     assertDataFrameEqual(output_data_object.df, expected_data)


def test_usual_environment_aggregation_uniform(spark, expected_data_uniform):
    """
    DESCRIPTION:
        Test shall execute the UsualEnvironmentAggregation component with uniform tile weights. The expected
        output is a dataframe with the total aggregated usual environments, homes and works in each grid tile.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/usual_environment_aggregation/usual_environment_aggregation_uniform.ini
        Input Data:
            enriched_grid_data_silver: /opt/testing_data/lakehouse/silver/grid_enriched
            usual_environment_labels_data_silver: /opt/testing_data/lakehouse/silver/usual_environment_labels

    OUTPUT:
        aggregated_usual_environments_silver:  /opt/testing_data/lakehouse/silver/aggregated_usual_environments

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiliary file
        3.- Init the UsualEnvironmentAggregation component with the test configs
        4.- Execute the UsualEnvironmentAggregation (includes read, transform, write)
        5.- Read resulting SilverAggregatedUsualEnvironmentsDataObject.
        6.- Load expected SilverAggregatedUsualEnvironmentsDataObject.
        7.- Assert DataFrames are equal.
    """
    # ========================================================================================================
    # TEST TAKING INTO ACCOUNT UNIFORM GRID TILE WEIGHTS
    # ========================================================================================================
    # Setup

    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/usual_environment_aggregation/usual_environment_aggregation_uniform.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data
    set_input_data_uniform(spark, config)

    ## Init component class
    component = UsualEnvironmentAggregation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected (defined as fixture)

    # Execution
    component.execute()

    # Assertion
    # read from test data output
    output_data_object = component.output_data_objects[SilverAggregatedUsualEnvironmentsDataObject.ID]
    output_data_object.read()

    # print(output_data_object.df.show(50))
    # print(expected_data.show(50))

    # assert read data == expected
    assertDataFrameEqual(output_data_object.df, expected_data_uniform)
