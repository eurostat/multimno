from configparser import ConfigParser
from pyspark.sql import SparkSession, Row

from pyspark.testing.utils import assertDataFrameEqual
from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labeling_quality_metrics_data_object import (
    SilverUsualEnvironmentLabelingQualityMetricsDataObject,
)
from multimno.components.execution.usual_environment_labeling.usual_environment_labeling import UsualEnvironmentLabeling
from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_common import (
    TEST_RESOURCES_PATH,
    TEST_GENERAL_CONFIG_PATH,
)
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir
from tests.test_code.multimno.components.execution.usual_environment_labeling.aux_test_usual_environment_labeling import (
    generate_input_lps_data,
    generate_expected_ue_labels,
    generate_expected_qm,
)

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    test_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    """

    ### Write input midterm permanence data to test resources dir
    longterm_permanence_data_path = config["Paths.Silver"]["longterm_permanence_score_data_silver"]
    input_do = SilverLongtermPermanenceScoreDataObject(spark, longterm_permanence_data_path)
    input_do.df = spark.createDataFrame(test_data, schema=SilverLongtermPermanenceScoreDataObject.SCHEMA)
    input_do.write()


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
    test_data = generate_input_lps_data("2023-01", "2023-02")
    set_input_data(spark, parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path), test_data)
    ## Init component class
    ue_labeling = UsualEnvironmentLabeling(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Expected labels
    expected_labels_data = generate_expected_ue_labels("2023-01", "2023-02")
    expected_sdf = spark.createDataFrame(expected_labels_data, schema=SilverUsualEnvironmentLabelsDataObject.SCHEMA)

    # Expected quality metrics
    expected_qm_data = generate_expected_qm("2023-01", "2023-02")
    expected_qm_sdf = spark.createDataFrame(
        expected_qm_data, schema=SilverUsualEnvironmentLabelingQualityMetricsDataObject.SCHEMA
    )

    # Execution
    ue_labeling.execute()

    # Assertion
    # read from test data output
    output_labels_data_object = ue_labeling.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
    output_labels_data_object.read()
    assertDataFrameEqual(output_labels_data_object.df, expected_sdf)

    output_qa_data_object = ue_labeling.output_data_objects[SilverUsualEnvironmentLabelingQualityMetricsDataObject.ID]
    output_qa_data_object.read()
    assertDataFrameEqual(output_qa_data_object.df, expected_qm_sdf)
