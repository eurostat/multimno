import pytest
import pandas as pd
import hashlib
from configparser import ConfigParser
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from multimno.core.constants.columns import ColNames
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
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_warnings_for_plots import (
    SilverEventDataSyntacticQualityWarningsForPlots,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]

# expceted SilverEventDataSyntacticQualityWarningsLogTable with three QWs
# two - for error_rate by date  (error_rate_upper_variability + error_rate_upper_limit)
# one - for clean_data_size (size_data_variability)


@pytest.fixture
def expected_event_cleaning_qw_log_table(spark):
    expected_log_table_data = [
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.error_rate.format(variables=ColNames.date),
            lookback_period="week",
            daily_value=80.0,
            condition_value=0.0,
            condition=Conditions.error_rate_upper_variability.format(variables=ColNames.date, SD="2"),
            warning_text=Warnings.error_rate_upper_variability.format(variables=ColNames.date),
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.error_rate.format(variables=ColNames.date),
            lookback_period="week",
            daily_value=80.0,
            condition_value=20.0,
            condition=Conditions.error_rate_upper_limit.format(variables=ColNames.date, X="20"),
            warning_text=Warnings.error_rate_upper_limit.format(variables=ColNames.date),
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.size_data.format(type_of_data="clean"),
            lookback_period="week",
            daily_value=100.0,
            condition_value=500.0,
            condition=Conditions.size_data_variability.format(SD="3"),
            warning_text=Warnings.size_data_variability.format(type_of_data="clean"),
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            measure_definition=MeasureDefinitions.error_type_rate.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
            lookback_period="week",
            daily_value=80.0,
            condition_value=0.0,
            condition=Conditions.error_type_rate_upper_variability.format(
                error_type_name="Deduplication same locations rate",
                field_name="None",
                SD="2",
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
                error_type_name="Deduplication same locations rate",
                field_name="None",
                X="20",
            ),
            warning_text=Warnings.error_type_rate_upper_limit.format(
                error_type_name="Deduplication same locations rate", field_name="None"
            ),
        ),
    ]

    expected_log_table_data_df = spark.createDataFrame(
        expected_log_table_data,
        schema=SilverEventDataSyntacticQualityWarningsLogTable.SCHEMA,
    )
    return expected_log_table_data_df


# expected SilverEventDataSyntacticQualityWarningsForPlots
# 30 rows, 10 each for raw_data_size, clean_data_size, and error_rate_by_date


@pytest.fixture
def expected_event_cleaning_qw_for_plots(spark):
    expected_for_plots_data = [
        Row(
            date=datetime.strptime("2024-01-01", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=None,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-03", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-04", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-05", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-06", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-07", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-08", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-09", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            type_of_qw="clean_data_size",
            lookback_period="week",
            daily_value=100.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-01", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=None,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-03", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-04", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-05", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-06", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-07", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-08", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-09", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=0.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            type_of_qw="error_rate",
            lookback_period="week",
            daily_value=80.0,
            average=0.0,
            LCL=None,
            UCL=0.0,
        ),
        Row(
            date=datetime.strptime("2024-01-01", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=None,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-02", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=None,
            UCL=None,
        ),
        Row(
            date=datetime.strptime("2024-01-03", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-04", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-05", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-06", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-07", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-08", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-09", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
        Row(
            date=datetime.strptime("2024-01-10", "%Y-%m-%d"),
            type_of_qw="raw_data_size",
            lookback_period="week",
            daily_value=500.0,
            average=500.0,
            LCL=500.0,
            UCL=500.0,
        ),
    ]

    expected_for_plots_data_df = spark.createDataFrame(
        expected_for_plots_data,
        schema=SilverEventDataSyntacticQualityWarningsForPlots.SCHEMA,
    )
    return expected_for_plots_data_df


def set_input_event_cleaning_qm_freq_distr(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data. The data should have schema of SilverEventDataSyntacticQualityMetricsFrequencyDistribution DO

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    # the idea is to create identical records except for tha last one
    # the last row (or date of interest) should have a significant drop in final frequency
    # so both clean_data_size and error_rate_by_date QW group will have records in LogTable
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

    event_cleaning_qm_freq_distr_df = pd.DataFrame(data_freq_dist)

    test_input_data_event_cleaning_qm_freq_dist_path = config["Paths.Silver"][
        "event_syntactic_quality_metrics_frequency_distribution"
    ]

    input_data_freq_dist_df = spark.createDataFrame(
        event_cleaning_qm_freq_distr_df,
        schema=SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA,
    )
    input_data = SilverEventDataSyntacticQualityMetricsFrequencyDistribution(
        spark, test_input_data_event_cleaning_qm_freq_dist_path
    )
    input_data.df = input_data_freq_dist_df
    input_data.write()


def set_input_event_cleaning_qm_by_column(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data. The data should have schema of SilverEventDataSyntacticQualityMetricsByColumn DO.
    Since the test for event_cleaning_quality_warnings does not include error_type_rate QW group,
    the purpose of this input is to exist and be not empty, otherwise EventQualityWanrings will raise Error when initialised

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    # Deduplication error input
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

    data_by_column_deduplication = [
        Row(
            result_timestamp=result_timestamp[i],
            date=date[i],
            variable=variable[i],
            type_of_error=type_of_error[i],
            type_of_transformation=type_of_transformation[i],
            value=value[i],
        )
        for i in range(num_records)
    ]

    # Other error type input
    data_by_column_other = [
        Row(
            result_timestamp=datetime.strptime("2024-01-15T10:00:00", "%Y-%m-%dT%H:%M:%S"),
            date=datetime(2024, 1, 1),
            variable="cell_id",
            type_of_error=2,
            type_of_transformation=1,
            value=150,
        )
    ]

    data_by_column = data_by_column_other + data_by_column_deduplication

    test_input_data_event_cleaning_qm_by_column_path = config["Paths.Silver"][
        "event_syntactic_quality_metrics_by_column"
    ]

    input_data_by_column_df = spark.createDataFrame(
        data_by_column, schema=SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA
    )

    input_data = SilverEventDataSyntacticQualityMetricsByColumn(spark, test_input_data_event_cleaning_qm_by_column_path)
    input_data.df = input_data_by_column_df
    input_data.write()
