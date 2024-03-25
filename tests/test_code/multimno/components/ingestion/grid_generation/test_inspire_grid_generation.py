from pyspark.testing.utils import assertDataFrameEqual
import pytest
from datetime import datetime
from pyspark.sql.types import Row, FloatType, StringType
from pyspark.sql import functions as F

from sedona.sql import st_constructors as STC

from multimno.components.ingestion.grid_generation.inspire_grid_generation import InspireGridGeneration
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject

from tests.test_code.multimno.components.ingestion.grid_generation.reference_data import REFERENCE_GRID
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir, assert_sparkgeodataframe_equal


# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_data(spark):
    """
    Aux function to setup expected data

    Args:
        spark (SparkSession): spark session
    """

    expected_sdf = spark.createDataFrame(REFERENCE_GRID)
    expected_sdf = expected_sdf.withColumn("geometry", STC.ST_GeomFromEWKT(F.col("geometry")))
    expected_sdf = (
        expected_sdf.withColumn("elevation", F.lit(None).cast(FloatType()))
        .withColumn("land_use", F.lit(None).cast(StringType()))
        .withColumn("prior_probability", F.lit(None).cast(FloatType()))
    )
    expected_sdf = expected_sdf.select(*[field.name for field in SilverGridDataObject.SCHEMA.fields])

    return expected_sdf


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


@pytest.mark.skip(reason="TODO: New Grid implementation")
def test_inspire_grid_generation(spark, expected_data):
    """
    DESCRIPTION:
        Test shall execute the InspireGridGeneration component with a given extent. The expected otuput is
        an INSPIRE grid of 100m x 100m tiles covering the extent with tile centroids geometry with
        additional columns of SilverGridDataObject schema.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/config/grid/grid_generation.ini
        Input Data:

    OUTPUT:
        grid_data_silver:  /opt/testing_data/lakehouse/grid

    STEPS:
        1.- Parse the configuration
        2.- Generate the input data using functions from the auxiallary file
        3.- Init the InspireGridGeneration component with the test configs
        4.- Execute the InspireGridGeneration (includes read, transform, write)
        5.- Read written Grid data object with InspireGridGeneration class.
        7.- Assert DataFrames are equal using geodataframe comparions utility function.
    """
    # Setup

    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/grid/grid_generation.ini"

    ## Init component class
    test_component = InspireGridGeneration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected (defined as fixture)

    # Execution
    test_component.execute()

    # Assertion
    # read from test data output
    output_footprint_data_object = test_component.output_data_objects[SilverGridDataObject.ID]
    output_footprint_data_object.read()

    output_intersection_groups_data_object = test_component.output_data_objects[SilverGridDataObject.ID]
    output_intersection_groups_data_object.read()

    # assert read data == expected
    assert_sparkgeodataframe_equal(output_footprint_data_object.df, expected_data)
