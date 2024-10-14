"""
Module that generates the quality warnings associated to the syntactic checks/cleaning of the raw MNO Network Topology Data.
"""

import pyspark.sql.functions as F
from pyspark.sql import Row
import datetime
import math

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import NetworkErrorType
from multimno.core.constants.network_default_thresholds import NETWORK_DEFAULT_THRESHOLDS
from multimno.core.data_objects.silver.silver_network_data_syntactic_quality_metrics_by_column import (
    SilverNetworkDataQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_network_syntactic_quality_warnings_log_table import (
    SilverNetworkDataSyntacticQualityWarningsLogTable,
)

from multimno.core.data_objects.silver.silver_network_syntactic_quality_warnings_plot_data import (
    SilverNetworkSyntacticQualityWarningsLinePlotData,
    SilverNetworkSyntacticQualityWarningsPiePlotData,
)

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY


class NetworkQualityWarnings(Component):
    """
    Class that produces the log tables and data required for plotting associated to Network Topology Data
    cleaning/syntactic checks.
    """

    COMPONENT_ID = "NetworkQualityWarnings"

    PERIOD_DURATION = {"week": 7, "month": 30, "quarter": 90}

    TITLE = "MNO Network Topology Data Quality Warnings"

    MEASURE_DEFINITION = {
        "SIZE_RAW_DATA": "Value of the size of the raw data object",
        "SIZE_CLEAN_DATA": "Value of the size of the clean data object",
        "TOTAL_ERROR_RATE": "Error rate",
        "Missing_value_RATE": "Missing rate value of {field_name}".format,
        "Out_of_range_RATE": "Out of range rate of {field_name}".format,
        "Parsing_error_RATE": "Parsing error rate of {field_name}".format,
    }

    ERROR_TYPE = {
        "Missing_value_RATE": NetworkErrorType.NULL_VALUE,
        "Out_of_range_RATE": NetworkErrorType.OUT_OF_RANGE,
        "Parsing_error_RATE": NetworkErrorType.CANNOT_PARSE,
    }

    CONDITION = {
        "Missing_value_RATE": {
            "AVERAGE": "Missing value rate of {field_name} is over the previous period average by more than {value} %".format,
            "VARIABILITY": (
                "Missing value rate of {field_name} is over the upper control limit calculated on the basis of average and standard deviation "
                + "of the distribution of the missing value rate of {field_name} in the previous period. Upper control limit = (average + {variability}·stddev)"
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "Missing value rate of {field_name} is over the value {value}".format,
        },
        "Out_of_range_RATE": {
            "AVERAGE": "Out of range rate of {field_name} is over the previous period average by more than {value} %".format,
            "VARIABILITY": (
                "Out of range rate of {field_name} is over the upper control limit calculated on the basis of average and standard deviation "
                + "of the distribution of the out of range rate of {field_name} in the previous period. Upper control limit = (average + {variability}·stddev)"
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "Out of range rate of {field_name} is over the value {value} %".format,
        },
        "Parsing_error_RATE": {
            "AVERAGE": "Parsing error rate of {field_name} is over the previous period average by more than {value} %".format,
            "VARIABILITY": (
                "Parsing error rate of {field_name} is over the upper control limit calculated on the basis of average and standard deviation "
                + "of the distribution of the parsing error rate of {field_name} in the previous period. Upper control limit = (average + {variability}·stddev)"
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "Parsing error rate of {field_name} is over the value {value}".format,
        },
    }

    WARNING_MESSAGE = {
        "Missing_value_RATE": {
            "AVERAGE": "The missing value rate of {field_name} after the syntactic check procedure is unexpectedly high with respect to the previous period.".format,
            "VARIABILITY": (
                "The missing value rate of {field_name} after the syntactic check procedure is unexpectedly high "
                + "with respect to previous period taking into account its usual variability."
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "The missing value rate of {field_name} is over the threshold".format,
        },
        "Out_of_range_RATE": {
            "AVERAGE": "The out of range rate of {field_name} after the syntactic check procedure is unexpectedly high with respect to the previous period.".format,
            "VARIABILITY": (
                "The out of range of {field_name} after the syntactic check procedure is unexpectedly high "
                + "with respect to previous period taking into account its usual variability."
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "The out of range rate of {field_name} is over the threshold".format,
        },
        "Parsing_error_RATE": {
            "AVERAGE": "The parsing error rate of {field_name} after the syntactic check procedure is unexpectedly high with respect to the previous period.".format,
            "VARIABILITY": (
                "The parsing error of {field_name} after the syntactic check procedure is unexpectedly high "
                + "with respect to previous period taking into account its usual variability."
            ).format,
            "ABS_VALUE_UPPER_LIMIT": "The parsing error rate of {field_name} is over the threshold".format,
        },
    }

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.timestamp = datetime.datetime.now()

        # Read date of study
        self.date_format = self.config.get(self.COMPONENT_ID, "date_format")
        date_of_study = self.config.get(self.COMPONENT_ID, "date_of_study")

        try:
            self.date_of_study = datetime.datetime.strptime(date_of_study, self.date_format).date()
        except ValueError as e:
            self.logger.error(str(e), exc_info=True)
            raise e

        # Read lookback period
        self.lookback_period = self.config.get(self.COMPONENT_ID, "lookback_period")

        if self.lookback_period not in ["week", "month", "quarter"]:
            error_msg = (
                "Configuration parameter `lookback_period` must be one of `week`, `month`, or `quarter`, "
                f"but {self.lookback_period} was passed"
            )

            self.logger.error(error_msg)
            raise ValueError(error_msg)

        self.lookback_dates = [
            self.date_of_study - datetime.timedelta(days=d)
            for d in range(1, self.PERIOD_DURATION[self.lookback_period] + 1)
        ]

        self.lookback_period_start = min(self.lookback_dates)
        self.lookback_period_end = max(self.lookback_dates)

        # Read thresholds and use read values instead of default ones when appropriate
        self.thresholds = self.get_thresholds()

        self.warnings = []

        self.plots_data = dict()

    def get_thresholds(self) -> dict:
        """
        Method that reads the threshold-related parameters, contained in the config file,
        and saves them in memory to use them instead of the ones specified by default.
        Raises:
            ValueError: non-numerical value that cannot be parsed to float has been used in
                the config file
            ValueError: Negative value for a given parameter has been given, when only
                non-negative values make sense and are thus allowed.

        Returns:
            thresholds: dict containing the different thresholds used for computing the
                quality warnings, with the same structure as
                multimno.core.constants.network_default_thresholds.NETWORK_DEFAULT_THRESHOLDS
        """
        config_thresholds = self.config.geteval(self.COMPONENT_ID, "thresholds")
        thresholds = NETWORK_DEFAULT_THRESHOLDS

        for error_key in config_thresholds.keys():
            if error_key not in NETWORK_DEFAULT_THRESHOLDS:
                self.logger.info(f"Parameter key {error_key} unknown -- ignored")
                continue

            if error_key in ["SIZE_RAW_DATA", "SIZE_CLEAN_DATA", "TOTAL_ERROR_RATE"]:
                for param_key, val in config_thresholds[error_key].items():
                    if param_key not in NETWORK_DEFAULT_THRESHOLDS[error_key]:
                        self.logger.info(f"Unknown parameter {param_key} under {error_key} -- ignored")
                        continue

                    try:
                        val = float(val)
                    except ValueError as e:
                        error_msg = (
                            f"Expected numeric value for parameter {param_key} under {error_key} - got {val} instead."
                        )
                        self.logger.error(error_msg)
                        raise e

                    if val < 0:
                        error_msg = f"Parameter {param_key} under {error_key} must be non-negative - got {val}"
                        self.logger.error(error_msg)
                        raise ValueError(error_msg)

                    thresholds[error_key][param_key] = val

            else:  # then is one of 'Missing_value_RATE', 'Out_of_range_RATE', 'Parsing_error_RATE'
                for field_name in config_thresholds[error_key]:
                    if field_name not in NETWORK_DEFAULT_THRESHOLDS[error_key]:
                        self.logger.info(f"Unknown field name {field_name} under {error_key} -- ignored")
                        continue

                    for param_key, val in config_thresholds[error_key][field_name].items():
                        if param_key not in NETWORK_DEFAULT_THRESHOLDS[error_key][field_name]:
                            self.logger.info(
                                f'Unknown parameter {param_key} under {error_key}["{field_name}"] -- ignored'
                            )
                            continue

                        try:
                            val = float(val)
                        except ValueError as e:
                            error_msg = f'Expected numeric value for parameter {param_key} under {error_key}["{field_name}"] - got {val} instead.'
                            self.logger.error(error_msg)
                            raise e

                        if val < 0:
                            error_msg = f'Parameter {param_key} under {error_key}["{field_name}"] must be non-negative - got {val}'
                            self.logger.error(error_msg)
                            raise ValueError(error_msg)

                        thresholds[error_key][field_name][param_key] = val

        return thresholds

    def initalize_data_objects(self):
        input_silver_quality_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_metrics_by_column"
        )
        output_silver_log_table_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_warnings_log_table"
        )

        output_silver_line_plot_data_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_warnings_line_plot_data"
        )

        output_silver_pie_plot_data_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_warnings_pie_plot_data"
        )

        silver_quality_metrics = SilverNetworkDataQualityMetricsByColumn(self.spark, input_silver_quality_metrics_path)

        silver_log_table = SilverNetworkDataSyntacticQualityWarningsLogTable(self.spark, output_silver_log_table_path)

        silver_line_plot_data = SilverNetworkSyntacticQualityWarningsLinePlotData(
            self.spark, output_silver_line_plot_data_path
        )

        silver_pie_plot_data = SilverNetworkSyntacticQualityWarningsPiePlotData(
            self.spark, output_silver_pie_plot_data_path
        )

        self.input_data_objects = {silver_quality_metrics.ID: silver_quality_metrics}
        self.output_data_objects = {
            silver_log_table.ID: silver_log_table,
            silver_line_plot_data.ID: silver_line_plot_data,
            silver_pie_plot_data.ID: silver_pie_plot_data,
        }

    def transform(self):

        # Check if both the date of study and the specified lookback period dates are in file
        self.check_needed_dates()

        lookback_stats, lookback_initial_rows, lookback_final_rows = self.get_lookback_period_statistics()

        today_values = self.get_study_date_values()

        raw_average, raw_UCL, raw_LCL = self.raw_size_warnings(lookback_stats, today_values)

        clean_average, clean_UCL, clean_LCL = self.clean_size_warnings(lookback_stats, today_values)

        error_rate, error_rate_avg, error_rate_UCL = self.error_rate_warnings(
            lookback_initial_rows, lookback_final_rows, today_values
        )

        self.all_specific_error_warnings(lookback_stats, today_values)

        self.output_data_objects[SilverNetworkDataSyntacticQualityWarningsLogTable.ID].df = self.spark.createDataFrame(
            self.warnings, SilverNetworkDataSyntacticQualityWarningsLogTable.SCHEMA
        )

        lookback_initial_rows[self.date_of_study] = today_values[None][NetworkErrorType.INITIAL_ROWS]
        lookback_final_rows[self.date_of_study] = today_values[None][NetworkErrorType.FINAL_ROWS]

        self.create_plots_data(
            lookback_initial_rows=lookback_initial_rows,
            lookback_final_rows=lookback_final_rows,
            today_values=today_values,
            error_rate=error_rate,
            raw_average=raw_average,
            clean_average=clean_average,
            error_rate_avg=error_rate_avg,
            raw_UCL=raw_UCL,
            clean_UCL=clean_UCL,
            error_rate_UCL=error_rate_UCL,
            raw_LCL=raw_LCL,
            clean_LCL=clean_LCL,
        )

    def check_needed_dates(self) -> None:
        """
        Method that checks if both the date of study and the dates necessary to generate
        the quality warnings, specified through the lookback_period parameter, are present
        in the input data.
        """

        # Collect all distinct dates in the input quality metrics within the needed range
        metrics = self.input_data_objects[SilverNetworkDataQualityMetricsByColumn.ID].df
        dates = (
            metrics.filter(
                F.col("date")
                # left- and right- inclusive
                .between(
                    self.date_of_study - datetime.timedelta(days=self.PERIOD_DURATION[self.lookback_period]),
                    self.date_of_study,
                )
            )
            .select(F.col(ColNames.date))
            .distinct()
            .collect()
        )

        dates = [row[ColNames.date] for row in dates]

        if self.date_of_study not in dates:
            raise ValueError(
                f"The date of study, {self.date_of_study}, is not present in the input Quality Metrics data"
            )

        if set(self.lookback_dates).intersection(set(dates)) != set(self.lookback_dates):
            error_msg = f"""
                The following dates from the lookback period are not present in the
                input Quality Metrics data:
                {
                    sorted(
                        map(
                            lambda x: x.strftime(self.date_format),
                            set(self.lookback_dates).difference(set(dates))
                        )
                    )
                }"""

            self.logger.error(error_msg)
            raise ValueError(error_msg)

    def get_lookback_period_statistics(self) -> dict:
        """Method that computes the necessary statistics (average and standard deviation) from the
        Quality Metrics of the lookback period.

        Returns:
            statistics (dict): dictionary containing said necessary statistics, with the following
                structure:
                {
                    field_name1: {
                        type_error1: {
                            'average': 12.2,
                            'stddev': 17.5
                        },
                        type_error2 : {
                            'average': 4.5,
                            'stddev': 10.1
                        }
                    },
                    ...
                }
            initial_rows (dict): dictionary indicating the number of rows before syntactic checks for the previous period, {date -> size_raw_data}
            final_rows (dict): dictionary indicating the number of rows after syntactic checks for the previous period, {date -> size_raw_data}
        """
        intermediate_df = self.input_data_objects[SilverNetworkDataQualityMetricsByColumn.ID].df.filter(
            F.col(ColNames.date).between(self.lookback_period_start, self.lookback_period_end)
        )

        intermediate_df.cache()

        lookback_stats = (
            intermediate_df.groupBy([ColNames.field_name, ColNames.type_code])
            .agg(F.mean(F.col(ColNames.value)).alias("average"), F.stddev_samp(F.col(ColNames.value)).alias("stddev"))
            .collect()
        )

        error_rate_data = (
            intermediate_df.select([ColNames.type_code, ColNames.value, ColNames.date])
            .filter(F.col(ColNames.type_code).isin([NetworkErrorType.INITIAL_ROWS, NetworkErrorType.FINAL_ROWS]))
            .collect()
        )

        intermediate_df.unpersist()

        initial_rows = {}
        final_rows = {}

        for row in error_rate_data:
            if row[ColNames.type_code] == NetworkErrorType.INITIAL_ROWS:
                initial_rows[row[ColNames.date]] = row["value"]
            else:
                final_rows[row[ColNames.date]] = row["value"]

        statistics = dict()
        for row in lookback_stats:
            field_name, type_code, average, stddev = (
                row[ColNames.field_name],
                row[ColNames.type_code],
                row["average"],
                row["stddev"],
            )
            if field_name not in statistics:
                statistics[field_name] = dict()
            statistics[field_name][type_code] = {"average": average, "stddev": stddev}

        return statistics, initial_rows, final_rows

    def get_study_date_values(self) -> dict:
        """Method that reads and returns the quality metrics of the date of study.

        Returns:
            today_values (dict): dictionary containing said values, with the following
                structure:
                {
                    field_name1: {
                        type_error1: 123,
                        type_error2: 23,
                        type_error3: 0
                    },
                    field_name2: {
                        type_error1: 0,
                        type_error2: 0,
                        type_error3: 300
                    },
                }
        """
        today_metrics = (
            self.input_data_objects[SilverNetworkDataQualityMetricsByColumn.ID]
            .df.filter(F.col(ColNames.date) == F.lit(self.date_of_study))
            .select([ColNames.field_name, ColNames.type_code, ColNames.value])
        ).collect()

        today_values = {}

        for row in today_metrics:
            field_name, type_code, value = row[ColNames.field_name], row[ColNames.type_code], row[ColNames.value]

            if field_name not in today_values:
                today_values[field_name] = {}

            today_values[field_name][type_code] = value

        return today_values

    def register_warning(
        self, measure_definition: str, daily_value: float, condition: str, condition_value: float, warning_text: str
    ) -> None:
        """Method that abstracts away the creation in the correct format and data type, each of the quality warnings
        that will be recorded in the log table.

        Args:
            measure_definition (str): measure that raised the warning (e.g. Error rate)
            daily_value (float): measure's value in the date of study that raised the warning
            condition (str): test that was checked in order to raise the warning
            condition_value (float): value against which the date of study's daily_value was compared
            warning_text (str): verbose explanation of the condition being satisfied and the warning
                being raised
        """
        warning = {
            ColNames.title: self.TITLE,
            ColNames.date: self.date_of_study,
            ColNames.timestamp: self.timestamp,
            ColNames.measure_definition: measure_definition,
            ColNames.daily_value: float(daily_value),
            ColNames.condition: condition,
            ColNames.lookback_period: self.lookback_period,
            ColNames.condition_value: float(condition_value),
            ColNames.warning_text: warning_text,
            ColNames.year: self.date_of_study.year,
            ColNames.month: self.date_of_study.month,
            ColNames.day: self.date_of_study.day,
        }

        self.warnings.append(Row(**warning))

    def raw_size_warnings(self, lookback_stats, today_values):
        """Method that generates the quality warnings, that will be recorded in the output log table,
        for the metric regarding the initial number of rows of the raw input network topology data prior
        to the syntactic check procedure.

        A total of six warnings might be generated:
            - The study date's number of rows is greater than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's number of rows is smaller than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's number of rows is greater than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's number of rows is smaller than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's number of rows is greater than a config-specified threshold.
            - The study date's number of rows is smaller than a config-specified threshold.
        """
        current_val = today_values[None][NetworkErrorType.INITIAL_ROWS]
        previous_avg = lookback_stats[None][NetworkErrorType.INITIAL_ROWS]["average"]
        previous_std = lookback_stats[None][NetworkErrorType.INITIAL_ROWS]["stddev"]

        measure_definition = self.MEASURE_DEFINITION["SIZE_RAW_DATA"]

        over_average = self.thresholds["SIZE_RAW_DATA"]["OVER_AVERAGE"]
        under_average = self.thresholds["SIZE_RAW_DATA"]["UNDER_AVERAGE"]
        variability = self.thresholds["SIZE_RAW_DATA"]["VARIABILITY"]
        absolute_upper_control_limit = self.thresholds["SIZE_RAW_DATA"]["ABS_VALUE_UPPER_LIMIT"]
        absolute_lower_control_limit = self.thresholds["SIZE_RAW_DATA"]["ABS_VALUE_LOWER_LIMIT"]

        if previous_avg == 0:
            self.logger.warning(
                f"""
                The average of the number of rows in the raw input network topology data is 0
                for the lookback period especified ({self.lookback_period_start} to {self.lookback_period_end},
                 both included)
                """
            )
        else:
            pct_difference = 100 * current_val / previous_avg - 100
            if pct_difference > over_average:
                self.register_warning(
                    measure_definition,
                    pct_difference,
                    f"Size is greater than the previous period's average by more than {over_average} %",
                    over_average,
                    "The number of cells is unexpectedly high with respect to the previous period, please check if there have been issues in the network",
                )

            if pct_difference < -under_average:
                self.register_warning(
                    measure_definition,
                    pct_difference,
                    f"Size is smaller than the previous period's average by more than {under_average} %",
                    under_average,
                    "The number of cells is unexpectedly low with respect to the previous period, please check if there have been issues in the network.",
                )

        upper_control_limit = previous_avg + variability * previous_std
        lower_control_limit = previous_avg - variability * previous_std

        if current_val > upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                "Size is out of the upper control limit calculated on the basis of average and standard deviation of the distribution "
                f"of the size in the previous period. Upper control limit = (average + {variability}·stddev)",
                upper_control_limit,
                "The number of cells is unexpectedly high with respect to the previous period, taking into account the usual "
                "variability of the cell numbers, please check if there have been issues in the network.",
            )
        if current_val < lower_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                "Size is out of the lower control limit calculated on the basis of average and standard deviation of the distribution "
                f"of the size in the previous period. Lower control limit = (average - {variability}·stddev)",
                lower_control_limit,
                "The number of cells is unexpectedly low with respect to the previous period, taking into account the usual "
                "variability of the cell numbers, please check if there have been issues in the network.",
            )

        # Use of UCL and LCL as default values for the absolute limits
        if absolute_upper_control_limit is None:
            absolute_upper_control_limit = upper_control_limit
        if absolute_lower_control_limit is None:
            absolute_lower_control_limit = lower_control_limit

        if current_val > absolute_upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                f"The size is over the threshold {absolute_upper_control_limit}",
                absolute_upper_control_limit,
                "The number of cells is over the threshold, please check if there have been changes in the network.",
            )
        if current_val < absolute_lower_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                f"The size is under the threshold {absolute_lower_control_limit}",
                absolute_upper_control_limit,
                "The number of cells is under the threshold, please check if there have been changes in the network.",
            )

        return previous_avg, upper_control_limit, lower_control_limit

    def clean_size_warnings(self, lookback_stats, today_values):
        """Method that generates quality warnings, that will be recorded in the output log table,
        for the metric regarding the final number of rows of the raw input network topology data prior
        to the syntactic check procedure.

        A total of six warnings might be generated:
            - The study date's number of rows is greater than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's number of rows is smaller than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's number of rows is greater than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's number of rows is smaller than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's number of rows is greater than a config-specified threshold.
            - The study date's number of rows is smaller than a config-specified threshold.
        """
        current_val = today_values[None][NetworkErrorType.FINAL_ROWS]
        previous_avg = lookback_stats[None][NetworkErrorType.FINAL_ROWS]["average"]
        previous_std = lookback_stats[None][NetworkErrorType.FINAL_ROWS]["stddev"]

        measure_definition = self.MEASURE_DEFINITION["SIZE_CLEAN_DATA"]

        over_average = self.thresholds["SIZE_CLEAN_DATA"]["OVER_AVERAGE"]
        under_average = self.thresholds["SIZE_CLEAN_DATA"]["UNDER_AVERAGE"]
        variability = self.thresholds["SIZE_CLEAN_DATA"]["VARIABILITY"]
        absolute_upper_control_limit = self.thresholds["SIZE_CLEAN_DATA"]["ABS_VALUE_UPPER_LIMIT"]
        absolute_lower_control_limit = self.thresholds["SIZE_CLEAN_DATA"]["ABS_VALUE_LOWER_LIMIT"]

        if previous_avg == 0:
            self.logger.warning(
                f"""
                The average of the number of rows in the clean input network topology data after syntactic checks is 0
                for the lookback period especified ({self.lookback_period_start} to {self.lookback_period_end}, 
                both included)
                """
            )
        else:
            pct_difference = 100 * current_val / previous_avg - 100
            if pct_difference > over_average:
                self.register_warning(
                    measure_definition,
                    pct_difference,
                    f"Size is greater than the previous period's average by {over_average} %",
                    over_average,
                    "The number of cells after the syntactic checks procedure is unexpectedly high with respect to the previous period.",
                )

            if pct_difference < -under_average:
                self.register_warning(
                    measure_definition,
                    pct_difference,
                    f"Size is smaller than the previous period's average by {under_average} %",
                    under_average,
                    "The number of cells after the syntactic checks procedure is unexpectedly low with respect to the previous period.",
                )

        upper_control_limit = previous_avg + variability * previous_std
        lower_control_limit = previous_avg - variability * previous_std

        if current_val > upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                "Size is out of the upper control limit calculated on the basis of average and standard deviation of the distribution "
                f"of the size in the previous period. Upper control limit = (average + {variability}·stddev)",
                upper_control_limit,
                "The number of cells is unexpectedly high with respect to the previous period, taking into account the usual "
                "variability of the cell numbers, please check if there have been issues in the network.",
            )
        if current_val < lower_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                "Size is out of the lower control limit calculated on the basis of average and standard deviation of the distribution "
                f"of the size in the previous period. Lower control limit = (average - {variability}·stddev)",
                lower_control_limit,
                "The number of cells is unexpectedly low with respect to the previous period, taking into account the usual "
                "variability of the cell numbers, please check if there have been issues in the network.",
            )

        # Use of UCL and LCL as default values for the absolute limits
        if absolute_upper_control_limit is None:
            absolute_upper_control_limit = upper_control_limit
        if absolute_lower_control_limit is None:
            absolute_lower_control_limit = lower_control_limit

        if current_val > absolute_upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                f"The size is over the threshold {absolute_upper_control_limit}",
                absolute_upper_control_limit,
                "The number of cells after the syntactic checks procedure is over the threshold.",
            )
        if current_val < absolute_lower_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                f"The size is under the threshold {absolute_lower_control_limit}",
                absolute_upper_control_limit,
                "The number of cells after the syntactic checks procedure is under the threshold.",
            )

        return previous_avg, upper_control_limit, lower_control_limit

    def error_rate_warnings(self, initial_rows, final_rows, today_values):
        """Method that generates quality warnings, that will be recorded in the output log table,
        for the metric regarding the error rate observed in the syntactic check procedure.

        A total of three warnings might be generated:
            - The study date's error rate is greater than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's error rate is greater than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's error rate is greater than a config-specified threshold.
        """
        if len(initial_rows) != len(final_rows):
            raise ValueError(
                "Input Quality Metrics do not have information on the number of rows "
                "before and after syntactic checks on all dates considered!"
            )

        error_rate = {
            date: 100 * (initial_rows[date] - final_rows[date]) / initial_rows[date] for date in initial_rows.keys()
        }

        current_val = (
            100
            * (today_values[None][NetworkErrorType.INITIAL_ROWS] - today_values[None][NetworkErrorType.FINAL_ROWS])
            / today_values[None][NetworkErrorType.INITIAL_ROWS]
        )
        previous_avg = sum(error_rate[date] for date in self.lookback_dates) / len(self.lookback_dates)
        previous_std = math.sqrt(
            sum((error_rate[date] - previous_avg) ** 2 for date in self.lookback_dates) / (len(self.lookback_dates) - 1)
        )

        measure_definition = self.MEASURE_DEFINITION["TOTAL_ERROR_RATE"]

        over_average = self.thresholds["TOTAL_ERROR_RATE"]["OVER_AVERAGE"]
        variability = self.thresholds["TOTAL_ERROR_RATE"]["VARIABILITY"]
        absolute_upper_control_limit = self.thresholds["TOTAL_ERROR_RATE"]["ABS_VALUE_UPPER_LIMIT"]

        if previous_avg == 0:
            self.logger.warning(
                f"""
                The average error rate in the input network topology data 0
                for the lookback period especified ({self.lookback_period_start} to {self.lookback_period_end}, 
                both included)
                """
            )
        else:
            pct_difference = 100 * current_val / previous_avg - 100
            if pct_difference > over_average:
                self.register_warning(
                    measure_definition,
                    pct_difference,
                    f"Error rate is greater than the previous period's average by more than {over_average} %",
                    over_average,
                    "The error rate after the syntactic checks procedure is unexpectedly high with respect to the previous period.",
                )
        upper_control_limit = previous_avg + variability * previous_std

        if current_val > upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                "Error rate is over the upper control limit calculated on the basis of average and standard deviation of the distribution "
                f"of the size in the previous period. Upper control limit = (average + {variability}·stddev)",
                upper_control_limit,
                "The error rate after the syntactic checks procedure is unexpectedly high with respect to the previous period, taking into account the usual "
                "variability.",
            )

        if current_val > absolute_upper_control_limit:
            self.register_warning(
                measure_definition,
                current_val,
                f"The error rate is over the value {absolute_upper_control_limit}",
                absolute_upper_control_limit,
                "The error rate after the syntactic checks procedure is over the threshold.",
            )
        error_rate[self.date_of_study] = current_val
        return error_rate, previous_avg, upper_control_limit

    def all_specific_error_warnings(self, lookback_stats, today_values):
        """Parent method for the creation of warnings for each type of error rate

        lookback_stats (dict): contains error information of each date of the lookback period
        today_values (dict): contains error information of the date of study
        """
        error_rate_types = ["Missing_value_RATE", "Out_of_range_RATE", "Parsing_error_RATE"]

        for error_rate_type in error_rate_types:
            for field_name in self.thresholds[error_rate_type]:
                self.specific_error_warnings(error_rate_type, field_name, lookback_stats, today_values)

    def specific_error_warnings(self, error_rate_type, field_name, lookback_stats, today_values):
        """Method that generates the quality warnings, that will be recorded in the output log table,
        for the metric regarding a specific error type considered in the network syntactic checks.

        A total of three warnings might be generated:
            - The study date's error rate for this error and this field is greater than the average number of rows over the previous period
                by more than a threshold percentage.
            - The study date's error rate for this error and this field is greater than the average number of rows over the previous period
                by a config-specified number of standard deviations.
            - The study date's error rate for this error and this field is greater than a config-specified threshold.
        """
        if error_rate_type not in self.ERROR_TYPE:
            raise ValueError(f"Unknown error type for quality warning `{error_rate_type}`")

        network_error_type = self.ERROR_TYPE[error_rate_type]

        over_average = self.thresholds[error_rate_type][field_name]["AVERAGE"]
        variability = self.thresholds[error_rate_type][field_name]["VARIABILITY"]
        absolute_upper_control_limit = self.thresholds[error_rate_type][field_name]["ABS_VALUE_UPPER_LIMIT"]

        if network_error_type == NetworkErrorType.OUT_OF_RANGE and field_name is None:
            field_name = "dates"
        current_val = today_values[field_name][network_error_type]
        previous_avg = lookback_stats[field_name][network_error_type]["average"]
        previous_std = lookback_stats[field_name][network_error_type]["stddev"]

        if previous_avg == 0:
            self.logger.warning(
                f"""
                The average {error_rate_type} for field {field_name} in the input network topology data is 0 for the lookback period
                especified ({self.lookback_period_start} to {self.lookback_period_end}, both included)
                """
            )
        else:
            pct_difference = 100 * current_val / previous_avg - 100
            if pct_difference > over_average:
                self.register_warning(
                    self.MEASURE_DEFINITION[error_rate_type](field_name=field_name),
                    pct_difference,
                    self.CONDITION[error_rate_type]["AVERAGE"](field_name=field_name, value=over_average),
                    over_average,
                    self.WARNING_MESSAGE[error_rate_type]["AVERAGE"](field_name=field_name),
                )
        upper_control_limit = previous_avg + variability * previous_std

        if current_val > upper_control_limit:
            self.register_warning(
                self.MEASURE_DEFINITION[error_rate_type](field_name=field_name),
                current_val,
                self.CONDITION[error_rate_type]["VARIABILITY"](field_name=field_name, variability=variability),
                variability,
                self.WARNING_MESSAGE[error_rate_type]["VARIABILITY"](field_name=field_name),
            )

        if current_val > absolute_upper_control_limit:
            self.register_warning(
                self.MEASURE_DEFINITION[error_rate_type](field_name=field_name),
                current_val,
                self.CONDITION[error_rate_type]["ABS_VALUE_UPPER_LIMIT"](
                    field_name=field_name, value=absolute_upper_control_limit
                ),
                absolute_upper_control_limit,
                self.WARNING_MESSAGE[error_rate_type]["ABS_VALUE_UPPER_LIMIT"](field_name=field_name),
            )

    def create_plots_data(
        self,
        lookback_initial_rows,
        lookback_final_rows,
        today_values,
        error_rate,
        raw_average,
        clean_average,
        error_rate_avg,
        raw_UCL,
        clean_UCL,
        error_rate_UCL,
        raw_LCL,
        clean_LCL,
    ):
        """Method that takes the data needed to generate the required plots, and formats it for
        easier plotting and saving later on.

        Args:
            lookback_initial_rows (dict): contains data on the lookback period dates' number of rows before syntactic checks
            lookback_final_rows (dict): contains data on the lookback period dates' number of rows after syntactic checks
            today_values (dict): contains data on the date of study error counts and rows before and after the syntactic checks
            error_rate (dict): cotains data on the error rates for all lookback dates and date of study.
            raw_average (float): average of rows in the raw data before syntactic checks
            clean_average (float): average of rows in the clean data after syntactic checks
            error_rate_avg (float): average of the error rate observed in the syntactic checks
            raw_UCL (float): upper control limit for the rows in the raw data before syntactic checks
            clean_UCL (float): upper control limit for the rows in the clean data after syntactic checks
            error_rate_UCL (float): upper control limit for the error rate
            raw_LCL (float): lower control limit for the rows in the raw data before syntactic checks
            clean_LCL (float): lower control limit for the rows in the raw data after syntactic checks
        """
        # self.plots_data = {"rows_before_syntactic_check": [], "rows_after_syntactic_check": [], "error_rate": []}

        plots_data = {"line_plot": [], "pie_plot": []}

        for date in sorted(self.lookback_dates) + [self.date_of_study]:
            plots_data["line_plot"].extend(
                [
                    Row(
                        **{
                            ColNames.date: date,
                            ColNames.daily_value: float(lookback_initial_rows[date]),
                            ColNames.average: float(raw_average),
                            ColNames.UCL: float(raw_UCL),
                            ColNames.LCL: float(raw_LCL),
                            ColNames.variable: "rows_before_syntactic_check",
                            ColNames.year: self.date_of_study.year,
                            ColNames.month: self.date_of_study.month,
                            ColNames.day: self.date_of_study.day,
                            ColNames.timestamp: self.timestamp,
                        }
                    )
                ]
            )

            plots_data["line_plot"].extend(
                [
                    Row(
                        **{
                            ColNames.date: date,
                            ColNames.daily_value: float(lookback_final_rows[date]),
                            ColNames.average: float(clean_average),
                            ColNames.UCL: float(clean_UCL),
                            ColNames.LCL: float(clean_LCL),
                            ColNames.variable: "rows_after_syntactic_check",
                            ColNames.year: self.date_of_study.year,
                            ColNames.month: self.date_of_study.month,
                            ColNames.day: self.date_of_study.day,
                            ColNames.timestamp: self.timestamp,
                        }
                    )
                ]
            )

            plots_data["line_plot"].extend(
                [
                    Row(
                        **{
                            ColNames.date: date,
                            ColNames.daily_value: float(error_rate[date]),
                            ColNames.average: float(error_rate_avg),
                            ColNames.UCL: float(error_rate_UCL),
                            ColNames.LCL: None,
                            ColNames.variable: "error_rate",
                            ColNames.year: self.date_of_study.year,
                            ColNames.month: self.date_of_study.month,
                            ColNames.day: self.date_of_study.day,
                            ColNames.timestamp: self.timestamp,
                        }
                    )
                ]
            )

        # Now, the data for the pie charts
        # Ugly way to get the relation error_code -> error attribute name
        error_types = {getattr(NetworkErrorType, att): att for att in dir(NetworkErrorType) if not att.startswith("__")}

        for field_name, error_counts in today_values.items():
            if field_name in [None, "dates"]:
                continue

            # boolean check if this field had any errors or not
            field_has_errors = False

            for key in error_counts.keys():
                if key != NetworkErrorType.NO_ERROR:
                    if error_counts[key] > 0:
                        # self.plots_data[field_name].append(
                        #     field_name, error_types[key], error_counts[key]
                        # )
                        field_has_errors = True
                        plots_data["pie_plot"].append(
                            Row(
                                **{
                                    ColNames.type_code: error_types[key],
                                    ColNames.value: error_counts[key],
                                    ColNames.variable: field_name,
                                    ColNames.year: self.date_of_study.year,
                                    ColNames.month: self.date_of_study.month,
                                    ColNames.day: self.date_of_study.day,
                                    ColNames.timestamp: self.timestamp,
                                }
                            )
                        )
            if not field_has_errors:
                self.logger.info(f"Field `{field_name}` had no errors")
        self.output_data_objects[SilverNetworkSyntacticQualityWarningsLinePlotData.ID].df = self.spark.createDataFrame(
            plots_data["line_plot"], schema=SilverNetworkSyntacticQualityWarningsLinePlotData.SCHEMA
        )

        self.output_data_objects[SilverNetworkSyntacticQualityWarningsPiePlotData.ID].df = self.spark.createDataFrame(
            plots_data["pie_plot"], schema=SilverNetworkSyntacticQualityWarningsPiePlotData.SCHEMA
        )

    def write(self):
        # Write regular data objects
        self.logger.info("Writing data objects...")
        super().write()
        self.logger.info("Data objects written.")
