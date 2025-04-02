# TODO convert to cell prox estimation

from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from multimno.core.configuration import parse_configuration
from multimno.components.execution.cell_proximity_estimation.cell_proximity_estimation import CellProximityEstimation
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.data_objects.silver.silver_cell_distance_data_object import SilverCellDistanceDataObject

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.cell_proximity_estimation.aux_cell_proximity_estimation import *
from tests.test_code.test_common import (
    TEST_RESOURCES_PATH,
    TEST_GENERAL_CONFIG_PATH,
    STATIC_TEST_DATA_PATH,
)
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_cell_proximity_estimation(spark):
    """
    DESCRIPTION:
        Test shall execute the CellProximityEstimation component with fixed input cell footprint
        and compare operational results of cell overlaps and cell distances.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/cell_proximity_estimation/cell_proximity_estimation.ini
        Input Data:
            cell_footprint_data_silver:

    OUTPUT:
        cell_intersection_groups_data_silver:
        cell_distance_data_silver:

    """
    # Setup

    ## Init configs & paths
    CELLPROXIMITYESTIMATION_CONFIG_PATH = (
        f"{TEST_RESOURCES_PATH}/config/cell_proximity_estimation/cell_proximity_estimation.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, CELLPROXIMITYESTIMATION_CONFIG_PATH)

    # ## Create Input data
    set_input_data(spark, config)

    # ## Init component class
    cpe_component = CellProximityEstimation(TEST_GENERAL_CONFIG_PATH, CELLPROXIMITYESTIMATION_CONFIG_PATH)
    cpe_component.execute()

    # Assertion: cell intersection groups
    # read from test data output
    output_cell_intersection_groups_data_object = cpe_component.output_data_objects[
        SilverCellIntersectionGroupsDataObject.ID
    ]
    output_cell_intersection_groups_data_object.read()

    expected_output_cell_intersection_groups = get_expected_cell_intersection_groups_df()
    groups_df = spark.createDataFrame(
        expected_output_cell_intersection_groups, schema=SilverCellIntersectionGroupsDataObject.SCHEMA
    )

    assertDataFrameEqual(output_cell_intersection_groups_data_object.df, groups_df, checkRowOrder=False)

    # Assertion: cell distance
    # read from test data output
    output_cell_distance_data_object = cpe_component.output_data_objects[SilverCellDistanceDataObject.ID]
    output_cell_distance_data_object.read()

    expected_output_cell_distance = get_expected_output_cell_distance_df()
    distance_df = spark.createDataFrame(expected_output_cell_distance, schema=SilverCellDistanceDataObject.SCHEMA)
    assertDataFrameEqual(output_cell_distance_data_object.df, distance_df, checkRowOrder=False)
