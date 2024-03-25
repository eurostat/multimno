import pytest

from pyspark.testing.utils import assertDataFrameEqual
import pyspark.sql.functions as F

from multimno.core.configuration import parse_configuration
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from multimno.components.execution.event_deduplication.event_deduplication import EventDeduplication

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.multimno.components.execution.event_deduplication.aux_deduplication_testing import (
    expected_deduplicated_events,
    expected_qa_freq,
    expected_qa_by_col,
    set_syntatically_cleaned_event_data,
    generate_qa_col_values_for_dates_with_no_records,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [
    spark,
    expected_deduplicated_events,
    expected_qa_by_col,
    expected_qa_freq,
]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


def test_event_deduplication(
    spark,
    expected_deduplicated_events: pytest.fixture,
    expected_qa_by_col: pytest.fixture,
    expected_qa_freq: pytest.fixture,
):
    """
    DESCRIPTION:
        Test shall execute the EventDeduplication component with
        an input dataframe of 5 rows.
        Two rows are same location duplicates, two rows are different location duplicates.
        One row is not a duplicate.
        This will test for the two quality metrics objects and deduplicated events.

    INPUT:
        Test Configs:
            general: tests/test_resources/config/general_config.ini
            component: tests/test_resources/config/event_deduplication/testing_event_deduplication.ini
        Input Data:
            silver_mno_event_data: opt/testing_data/lakehouse/silver/mno_events
    EXPECTED OUTPUT:
        silver_mno_event_data: opt/testing_data/lakehouse/silver/mno_events_deduplicated
        silver_mno_event_data_quality_metrics_frequency_distribution: opt/testing_data/lakehouse/silver/quality_metrics/event_deduplicated_quality_metrics_frequency_distribution
        silver_mno_event_data_quality_metrics_by_column: opt/testing_data/lakehouse/silver/quality_metrics/event_deduplicated_quality_metrics_by_column


    STEPS:
        1.- Init the EventDeduplication component with test configs.
        2.- Read expected data with SilverEventDataObject class.
        3.- Execute the module.
        4.- Read written data in /opt/testing_data
        5.- Assert DataFrames are equal.
    """
    # Setup

    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/event_deduplication/testing_event_deduplication.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Create Input data

    set_syntatically_cleaned_event_data(spark, config)

    ## Init component class
    event_deduplication = EventDeduplication(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    event_deduplication.execute()

    # Assertion
    # read from test data output

    ## 3 assertion tests in total , as there are 3 output data objects

    # I deduplicated events

    # Deduplication output_data object is overwritten for each date iteration
    # this would give the output for the last date:
    # event_deduplication.output_data_objects[SilverEventDataObject.ID]
    # .write is called several times within .execute()
    # So instead, we read from the final written file

    output_events_df = spark.read.schema(SilverEventDataObject.SCHEMA).parquet(
        config["Paths.Silver"]["event_data_silver_deduplicated"]
    )
    assertDataFrameEqual(output_events_df, expected_deduplicated_events)

    # II QA frequency

    # Deduplication output_data object is overwritten for each date iteration
    # .write is called several times within .execute()
    # So instead, we read from the final written file

    output_freq_df = spark.read.schema(SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA).parquet(
        config["Paths.Silver"]["event_deduplicated_quality_metrics_frequency_distribution"]
    )
    assertDataFrameEqual(output_freq_df, expected_qa_freq)

    # III QA by column

    output_qa_col_df = spark.read.schema(SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA).parquet(
        config["Paths.Silver"]["event_deduplicated_quality_metrics_by_column"]
    )

    output_qa_col_df = output_qa_col_df.orderBy("date", "type_of_error")

    expected_zero_rows = generate_qa_col_values_for_dates_with_no_records(spark, config)

    expected_qa_by_col = expected_qa_by_col.union(expected_zero_rows).orderBy("date", "type_of_error")

    # No sorting required in this test case
    ## III.I Non timestamp columns assertion
    output_qa_col_df.persist()
    expected_qa_by_col.persist()

    # sort columns and cast to correct datatype
    columns = [field.name for field in SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA.fields]
    output_qa_col_df_left = output_qa_col_df.select(columns).drop("result_timestamp")
    expected_qa_by_col_right = expected_qa_by_col.select(columns).drop("result_timestamp")
    assertDataFrameEqual(output_qa_col_df_left, expected_qa_by_col_right)

    ## III.II Result timestamp assertion

    ## TODO considern alternatives which way to properly test the timestamp column here,
    ## Ignore it completely?
    ## Current option is to use the difference of timestamps, as testing should happen fast
    ## On any machine

    # TODO: Let's postpone this verification for a next release as it sensible.
    # test_expected_time = (
    #     expected_qa_by_col.withColumn("unix_timestamp", unix_timestamp("result_timestamp"))
    #     .select("unix_timestamp")
    #     .collect()
    # )

    # real_time = (
    #     output_qa_col_df.withColumn("unix_timestamp", unix_timestamp("result_timestamp"))
    #     .select("unix_timestamp")
    #     .collect()
    # )

    # conditions_list = []

    # # Current parameter: test run should be done in 1 minute

    # for i in range(len(real_time)):
    #     conditions_list.append((real_time[i].unix_timestamp - test_expected_time[i].unix_timestamp) < 60)

    # assert all(conditions_list)
