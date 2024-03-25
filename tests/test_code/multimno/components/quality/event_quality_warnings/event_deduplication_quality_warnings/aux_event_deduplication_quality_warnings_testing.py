import pytest
import pandas as pd
import hashlib
from configparser import ConfigParser
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from multimno.core.constants.error_types import ErrorTypes
from multimno.core.constants.measure_definitions import MeasureDefinitions
from multimno.core.constants.conditions import Conditions
from multimno.core.constants.warnings import Warnings

from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_warnings_log_table import (
    SilverEventDataSyntacticQualityWarningsLogTable,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]

# for deduplication only expected log table is needed
# because for error_type_rate QW group no plots are expected


@pytest.fixture
def expected_event_deduplication_qw_log_table(spark):
    expected_log_table_data = [
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.error_type_rate.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
            lookback_period="week",
            daily_value=80.0,
            condition_value=0.0,
            condition=Conditions.error_type_rate_upper_variability.format(
                error_type_name="Deduplication same locations rate", field_name="None", SD="2"
            ),
            warning_text=Warnings.error_type_rate_upper_variability.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.error_type_rate.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
            lookback_period="week",
            daily_value=80.0,
            condition_value=20.0,
            condition=Conditions.error_type_rate_upper_limit.format(
                error_type_name="Deduplication same locations rate", field_name="None", X="20"
            ),
            warning_text=Warnings.error_type_rate_upper_limit.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
        ),
    ]

    expected_log_table_data_df = spark.createDataFrame(
        expected_log_table_data, schema=SilverEventDataSyntacticQualityWarningsLogTable.SCHEMA
    )
    return expected_log_table_data_df


def set_input_event_deduplication_qm_freq_distr(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data. The data should have schema of SilverEventDataSyntacticQualityMetricsFrequencyDistribution DO

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    # since we are checking error_type_rate QW group don't really care about the content
    # except for initial_frequency, logically it could not be lower than number of errors for ErrorTypes.same_location_duplicate
    num_records = 10
    dummy_user_string = "user1234567890"
    dummy_cell_id = "12345678901234"
    start_date = datetime(2024, 1, 1)
    freq = 500
    last_final_freq = 100

    cell_id = [dummy_cell_id] * num_records
    user_id = [hashlib.sha256(dummy_user_string.encode("utf-8")).digest()] * num_records
    initial_frequency = [freq] * 10
    final_frequency = [freq] * (num_records - 1) + [last_final_freq]
    date = [start_date + timedelta(days=i) for i in range(num_records)]

    data_freq_dist = {
        "cell_id": cell_id,
        "user_id": user_id,
        "initial_frequency": initial_frequency,
        "final_frequency": final_frequency,
        "date": date,
    }

    event_deduplication_qm_freq_distr_df = pd.DataFrame(data_freq_dist)

    test_input_data_event_deduplication_qm_freq_dist_path = config["Paths.Silver"][
        "event_deduplicated_quality_metrics_frequency_distribution"
    ]

    input_data_freq_dist_df = spark.createDataFrame(
        event_deduplication_qm_freq_distr_df, schema=SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA
    )
    input_data = SilverEventDataSyntacticQualityMetricsFrequencyDistribution(
        spark, test_input_data_event_deduplication_qm_freq_dist_path
    )
    input_data.df = input_data_freq_dist_df
    input_data.write()


def set_input_event_deduplication_qm_by_column(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data. The data should have schema of SilverEventDataSyntacticQualityMetricsByColumn DO

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    # the idea is to create identical records except for tha last one
    # the last row (or date of interest) should have a lot of errors for ErrorTypes.same_location_duplicate
    # so upper_variability and upper_limit checks would be raised
    num_records = 10
    start_date = datetime(2024, 1, 1)
    value = 0
    last_value = 400

    result_timestamp = [datetime.strptime("2024-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S")] * num_records
    date = [start_date + timedelta(days=i) for i in range(num_records)]
    variable = [None] * num_records
    type_of_error = [ErrorTypes.same_location_duplicate] * num_records
    type_of_transformation = [None] * num_records
    value = [value] * (num_records - 1) + [last_value]

    data_by_column = {
        "result_timestamp": result_timestamp,
        "date": date,
        "variable": variable,
        "type_of_error": type_of_error,
        "type_of_transformation": type_of_transformation,
        "value": value,
    }

    event_deduplication_qm_by_column_df = pd.DataFrame(data_by_column)

    test_input_data_event_deduplication_qm_by_column_path = config["Paths.Silver"][
        "event_deduplicated_quality_metrics_by_column"
    ]

    input_data_by_column_df = spark.createDataFrame(
        event_deduplication_qm_by_column_df, schema=SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA
    )
    input_data = SilverEventDataSyntacticQualityMetricsByColumn(
        spark, test_input_data_event_deduplication_qm_by_column_path
    )
    input_data.df = input_data_by_column_df
    input_data.write()
