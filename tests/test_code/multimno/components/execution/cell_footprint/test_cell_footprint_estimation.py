from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)
from multimno.components.execution.cell_footprint.cell_footprint_estimation import (
    CellFootprintEstimation,
)
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.multimno.components.execution.cell_footprint.aux_cell_footprint_testing import (
    set_input_data,
    generate_expected_cell_footprint_data,
    generate_expected_network_impute_default_properties,
)
from tests.test_code.test_common import (
    TEST_RESOURCES_PATH,
    TEST_GENERAL_CONFIG_PATH,
)

from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]

CELLFOOTPRINT_CONFIG_PATH = (
    f"{TEST_RESOURCES_PATH}/config/network/cell_footprint_estimation/cell_footprint_estimation.ini"
)


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_cell_footprint_estimation(spark):
    """
    Test the cell footprint estimation component.

    DESCRIPTION:
    This test verifies the cell footprint estimation component. It initialises the necessary configurations, sets up the
    input data, executes the cell footprint estimation process, and asserts that the output DataFrame matches the expected
    result

    INPUT:
    - spark: Spark session fixture provided by pytest.

    STEPS:
    1. Initialise configuration path and configuration
    3. Create input datausing the test configuration
    4. Initialise the CellFootprintEstimation component with the test configuration
    5. Execute the component.
    6. Read the output of the component.
    7. Get the expected result of the execution.
    8. Assert equality of component output and expected output.
    """
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)
    set_input_data(spark, config)

    cfe = CellFootprintEstimation(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)

    cfe.execute()

    expected_df = spark.createDataFrame(generate_expected_cell_footprint_data(), SilverCellFootprintDataObject.SCHEMA)

    output_footprint_data_object = cfe.output_data_objects[SilverCellFootprintDataObject.ID]
    output_footprint_data_object.read()

    # assert read data == expected
    assertDataFrameEqual(output_footprint_data_object.df, expected_df)


def test_impute_default_cell_properties(spark):
    """
    DESCRIPTION:
        Test the impute_default_cell_properties method of CellFootprintEstimation class.

    INPUT:
        sdf: Input DataFrame with null values.

    OUTPUT:
        sdf_imputed: DataFrame with imputed default cell properties.
    """
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)
    set_input_data(spark, config)

    # Create instance of CellFootprintEstimation class
    cfe = CellFootprintEstimation(TEST_GENERAL_CONFIG_PATH, CELLFOOTPRINT_CONFIG_PATH)

    # Create test input DataFrame
    net_do = cfe.input_data_objects[SilverNetworkDataObject.ID]
    net_do.read()
    input_df = net_do.df

    # Call the impute_default_cell_properties method
    sdf_imputed = cfe.impute_default_cell_properties(input_df)

    # Define expected output DataFrame
    expected_sdf = spark.createDataFrame(
        generate_expected_network_impute_default_properties(), schema=sdf_imputed.schema
    )

    # Assert that the output DataFrame matches the expected DataFrame
    assertDataFrameEqual(sdf_imputed, expected_sdf)
