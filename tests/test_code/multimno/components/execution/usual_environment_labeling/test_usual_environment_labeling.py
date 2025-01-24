from pyspark.testing.utils import assertDataFrameEqual
from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.components.execution.usual_environment_labeling.usual_environment_labeling import UsualEnvironmentLabeling
from tests.test_code.fixtures import spark_session as spark
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


# def teardown_function():
#     teardown_test_data_dir()


def prepare_test_data(spark):
    """
    DESCRIPTION:
        Function to prepare the test data for the tests.
    """
    # Prepare test data
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH)
    longterm_permanence_do = SilverLongtermPermanenceScoreDataObject(
        spark, config.get(CONFIG_SILVER_PATHS_KEY, "longterm_permanence_score_data_silver")
    )
    longterm_permanence_sdf = spark.read.format("parquet").load(
        f"{STATIC_TEST_DATA_PATH}/longterm_analysis/longterm_permanence_score"
    )
    longterm_permanence_sdf.show()
    longterm_permanence_do.df = longterm_permanence_sdf
    longterm_permanence_do.write()


def test_usual_environment_labeling(spark):
    """
    DESCRIPTION:
        Test shall execute the UsualEnvironmentLabeling component with a test longterm permanence dataset.
        The test shall assert the output data is equal to the expected data.
    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/longterm_analysis/usual_environment_labeling.ini
        Input Data:
            longterm_permanence_score_data_silver: /opt/testing_data/lakehouse/silver/longterm_permanence_score
    OUTPUT:
        usual_environment_labels_data_silver: /opt/testing_data/lakehouse/usual_environment_labels
    """
    # Setup
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/longterm_analysis/usual_environment_labeling.ini"
    ## Create Input data
    prepare_test_data(spark)
    ## Init component class
    ue_labeling = UsualEnvironmentLabeling(TEST_GENERAL_CONFIG_PATH, component_config_path)
    # Expected
    expected_do = SilverUsualEnvironmentLabelsDataObject(
        spark, f"{STATIC_TEST_DATA_PATH}/longterm_analysis/expected_usual_environment_labels"
    )
    expected_do.read()
    expected_sdf = expected_do.df
    # Execution
    ue_labeling.execute()

    # Assertion
    # read from test data output
    output_data_object = ue_labeling.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
    output_data_object.read()

    # assert read data == expected
    assertDataFrameEqual(expected_sdf, output_data_object.df)
