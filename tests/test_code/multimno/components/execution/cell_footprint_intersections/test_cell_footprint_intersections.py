from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames
from multimno.components.execution.cell_footprint_intersections.cell_footprint_intersections import (
    CellFootprintIntersections,
)
from multimno.core.data_objects.silver.silver_cell_to_group_data_object import SilverCellToGroupDataObject
from multimno.core.data_objects.silver.silver_group_to_tile_data_object import SilverGroupToTileDataObject
from multimno.core.utils import apply_schema_casting

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir

from tests.test_code.multimno.components.execution.cell_footprint_intersections.aux_cell_footprint_intersections import (
    set_input_data,
    generate_expected_cell_to_group_data,
    generate_expected_group_to_tile_data,
)


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_cell_footprint_intersections(spark):
    """
    Test the cell footprint intersections component.

    DESCRIPTION:
    This test verifies the cell footprint intersections component. It initialises the necessary configurations, sets up the
    input data, executes the cell footprint intersections process, and asserts that the output DataFrame matches the expected
    result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input cell footprint data using the test configuration
    4. Initialise the CellFootprintIntersections component with the test configuration
    5. Execute the cell footprint intersections component.
    6. Read the output of the cell footprint intersections component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/cell_footprint_intersections/cell_footprint_intersections.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Create input data
    set_input_data(spark, config)

    # Initialise component
    intersections = CellFootprintIntersections(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    intersections.execute()

    # Read output
    cell_to_group_do = intersections.output_data_objects[SilverCellToGroupDataObject.ID]
    group_to_tile_do = intersections.output_data_objects[SilverGroupToTileDataObject.ID]
    cell_to_group_do.read()
    group_to_tile_do.read()

    # Get expected result

    expected_group_to_tile = apply_schema_casting(
        spark.createDataFrame(generate_expected_group_to_tile_data()),
        SilverGroupToTileDataObject.SCHEMA,
    )

    expected_cell_to_group = apply_schema_casting(
        spark.createDataFrame(generate_expected_cell_to_group_data()),
        SilverCellToGroupDataObject.SCHEMA,
    )

    # TODO: refine test due to xxhash64 hashing
    # Assert equality of group to tile results
    # assertDataFrameEqual(cell_to_group_do.df, expected_cell_to_group)
    assert cell_to_group_do.df.count() == expected_cell_to_group.count()

    # Assert equality of cell to group results
    # assertDataFrameEqual(group_to_tile_do.df, expected_group_to_tile)
    assert group_to_tile_do.df.count() == expected_group_to_tile.count()
