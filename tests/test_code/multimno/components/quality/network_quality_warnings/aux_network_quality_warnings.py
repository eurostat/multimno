import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
import datetime

from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import NetworkErrorType
from multimno.core.data_objects.silver.silver_network_data_syntactic_quality_metrics_by_column import (
    SilverNetworkDataQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_network_syntactic_quality_warnings_log_table import (
    SilverNetworkDataSyntacticQualityWarningsLogTable,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_log_table(spark):
    """Expected log table output DO of the test

    Args:
        spark (SparkSession): spark session

    Returns:
        SilverNetworkDataSyntacticQualityWarningsLogTable
    """
    title = "MNO Network Topology Data Quality Warnings"
    result_timestamp = datetime.datetime(year=2024, month=3, day=1, hour=12)  # useless
    date = datetime.date(year=2023, month=7, day=11)
    lookback_period = "week"
    expected_data = [
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Value of the size of the raw data object",
            daily_value=300.0,
            condition="The size is under the threshold 310.0",
            lookback_period=lookback_period,
            condition_value=310.0,
            warning_text="The number of cells is under the threshold, please check if there have been changes in the network.",
            year=2023,
            month=7,
            day=11,
        ),
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Value of the size of the clean data object",
            daily_value=280.0,
            condition="The size is under the threshold 310.0",
            lookback_period=lookback_period,
            condition_value=310.0,
            warning_text="The number of cells after the syntactic checks procedure is under the threshold.",
            year=2023,
            month=7,
            day=11,
        ),
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Missing rate value of frequency",
            daily_value=200.0,
            condition="Missing value rate of frequency is over the previous period average by more than 60.0 %",
            lookback_period=lookback_period,
            condition_value=60.0,
            warning_text="The missing value rate of frequency after the syntactic check procedure is unexpectedly high with respect to the previous period.",
            year=2023,
            month=7,
            day=11,
        ),
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Missing rate value of frequency",
            daily_value=6.0,
            condition="Missing value rate of frequency is over the upper control limit calculated on the basis of average and standard deviation of the distribution of the missing value rate of frequency in the previous period. Upper control limit = (average + 2.0·stddev)",
            lookback_period=lookback_period,
            condition_value=2.0,
            warning_text="The missing value rate of frequency after the syntactic check procedure is unexpectedly high with respect to previous period taking into account its usual variability.",
            year=2023,
            month=7,
            day=11,
        ),
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Out of range rate of frequency",
            daily_value=200.0,
            condition="Out of range rate of frequency is over the previous period average by more than 60.0 %",
            lookback_period=lookback_period,
            condition_value=60.0,
            warning_text="The out of range rate of frequency after the syntactic check procedure is unexpectedly high with respect to the previous period.",
            year=2023,
            month=7,
            day=11,
        ),
        Row(
            title=title,
            date=date,
            timestamp=result_timestamp,
            measure_definition="Out of range rate of frequency",
            daily_value=6.0,
            condition="Out of range rate of frequency is over the upper control limit calculated on the basis of average and standard deviation of the distribution of the out of range rate of frequency in the previous period. Upper control limit = (average + 2.0·stddev)",
            lookback_period=lookback_period,
            condition_value=2.0,
            warning_text="The out of range of frequency after the syntactic check procedure is unexpectedly high with respect to previous period taking into account its usual variability.",
            year=2023,
            month=7,
            day=11,
        ),
    ]

    expected_data_df = spark.createDataFrame(
        expected_data, schema=SilverNetworkDataSyntacticQualityWarningsLogTable.SCHEMA
    )
    return expected_data_df


def set_input_metrics_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    test_data_path = config["Paths.Silver"]["network_syntactic_quality_metrics_by_column"]

    field_codes = {
        "power": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "elevation_angle": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "directionality": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "longitude": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "vertical_beam_width": [NetworkErrorType.NULL_VALUE, NetworkErrorType.OUT_OF_RANGE],
        "antenna_height": [NetworkErrorType.NULL_VALUE, NetworkErrorType.OUT_OF_RANGE],
        "azimuth_angle": [NetworkErrorType.NULL_VALUE, NetworkErrorType.OUT_OF_RANGE],
        "cell_type": [NetworkErrorType.NULL_VALUE, NetworkErrorType.OUT_OF_RANGE],
        "valid_date_start": [NetworkErrorType.NULL_VALUE, NetworkErrorType.CANNOT_PARSE],
        "cell_id": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "altitude": [NetworkErrorType.NULL_VALUE],
        "technology": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "latitude": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        "horizontal_beam_width": [NetworkErrorType.NULL_VALUE, NetworkErrorType.OUT_OF_RANGE],
        "dates": [NetworkErrorType.OUT_OF_RANGE],
        "valid_date_end": [NetworkErrorType.CANNOT_PARSE, NetworkErrorType.NULL_VALUE],
        "frequency": [NetworkErrorType.OUT_OF_RANGE, NetworkErrorType.NULL_VALUE],
        None: [NetworkErrorType.INITIAL_ROWS, NetworkErrorType.FINAL_ROWS],
    }

    initial_rows = 300
    final_rows = 280
    # Fields cna have 1 or 2 error types. We set any particular field to have at most 4 errors, equally divided
    # between each possible code.
    errors_per_field_and_code = dict()
    for field in field_codes:
        if field is None:
            continue
        errors_per_field_and_code[field] = int(4 / len(field_codes[field]))

    def get_value(field, code):
        if field is None:
            if code == NetworkErrorType.INITIAL_ROWS:
                return initial_rows
            elif code == NetworkErrorType.FINAL_ROWS:
                return final_rows

        if code == NetworkErrorType.NO_ERROR:
            if field == "dates":
                raise ValueError("dates does not have NO_ERROR metric!")
            return initial_rows - 4
        else:
            return errors_per_field_and_code[field]

    result_timestamp = datetime.datetime(year=2024, month=3, day=1, hour=12)
    date = datetime.date(year=2023, month=7, day=11)
    dates = [datetime.date(year=2023, month=7, day=11) + datetime.timedelta(days=-(i + 1)) for i in range(7)]

    data = [
        Row(
            **{
                ColNames.result_timestamp: result_timestamp,
                ColNames.date: date,
                ColNames.field_name: field,
                ColNames.type_code: code,
                ColNames.value: get_value(field, code),
                ColNames.year: date.year,
                ColNames.month: date.month,
                ColNames.day: date.day,
            }
        )
        for date in dates
        for field in field_codes.keys()
        for code in field_codes[field]
    ]

    def study_date_values(field, code):
        if field is None:
            if code == NetworkErrorType.INITIAL_ROWS:
                return initial_rows
            elif code == NetworkErrorType.FINAL_ROWS:
                return final_rows

        if code == NetworkErrorType.NO_ERROR:
            if field == "dates":
                raise ValueError("dates does not have NO_ERROR metric!")
            return initial_rows - 4
        else:
            if field == ColNames.frequency:
                return 6
            return errors_per_field_and_code[field]

    data.extend(
        [
            Row(
                **{
                    ColNames.result_timestamp: result_timestamp,
                    ColNames.date: date,
                    ColNames.field_name: field,
                    ColNames.type_code: code,
                    ColNames.value: study_date_values(field, code),
                    ColNames.year: date.year,
                    ColNames.month: date.month,
                    ColNames.day: date.day,
                }
            )
            for field in field_codes.keys()
            for code in field_codes[field]
        ]
    )
    input_data_df = spark.createDataFrame(data, schema=SilverNetworkDataQualityMetricsByColumn.SCHEMA)

    # Write input data in test resoruces dir
    input_data = SilverNetworkDataQualityMetricsByColumn(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write()
