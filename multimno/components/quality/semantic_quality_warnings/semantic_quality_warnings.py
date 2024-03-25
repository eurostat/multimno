"""
"""

import pyspark.sql.functions as F
from pyspark.sql import Row
import pandas as pd
import datetime
import math

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.semantic_qw_default_thresholds import SEMANTIC_DEFAULT_THRESHOLDS
from multimno.core.constants.error_types import SemanticErrorType
from multimno.core.data_objects.silver.silver_semantic_quality_metrics import SilverEventSemanticQualityMetrics
from multimno.core.data_objects.silver.silver_semantic_quality_warnings_log_table import (
    SilverEventSemanticQualityWarningsLogTable,
)
from multimno.core.data_objects.silver.silver_semantic_quality_warnings_plot_data import (
    SilverEventSemanticQualityWarningsBarPlotData,
)

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY


class SemanticQualityWarnings(Component):
    """ """

    COMPONENT_ID = "SemanticQualityWarnings"

    MINIMUM_STD_LOOKBACK_DAYS = 3

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

        self.thresholds = self.get_thresholds()

        self.warning_long_format = []

    def get_thresholds(self) -> dict:
        """
        Method that reads the threshold-related parameters, contained in the config file,
        and saves them in memory to use them instead of the ones specified by default.

        Raises:
            ValueError: non-numerical value that cannot be parsed to float (or int) has been used in
                the config file
            ValueError: Negative value for a given parameter has been given, when only
                non-negative values make sense and are thus allowed.

        Returns:
            thresholds: dict containing the different thresholds used for computing the
                quality warnings, with the same structure as
                multimno.core.constants.network_default_thresholds.ASEMANTIC_DEFAULT_THRESHOLDS
        """
        config_thresholds = self.config.geteval(self.COMPONENT_ID, "thresholds")
        thresholds = SEMANTIC_DEFAULT_THRESHOLDS

        for error_key in config_thresholds.keys():
            if error_key not in SEMANTIC_DEFAULT_THRESHOLDS:
                self.logger.info(f"Parameter key {error_key} unknown -- ignored")
                continue

            for param_key, val in config_thresholds[error_key].items():
                if param_key not in SEMANTIC_DEFAULT_THRESHOLDS[error_key]:
                    self.logger.info(f"Unknown parameter {param_key} under {error_key} -- ignored")
                    continue

                if param_key == "sd_lookback_days":
                    try:
                        val = int(val)
                    except ValueError as e:
                        error_msg = (
                            f"Expected integer value for parameter {param_key} under {error_key} - got {val} instead."
                        )
                        self.logger.error(error_msg)
                        raise e
                else:
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

        return thresholds

    def initalize_data_objects(self):
        input_silver_quality_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_device_semantic_quality_metrics"
        )
        output_silver_log_table_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_device_semantic_quality_warnings_log_table"
        )
        output_silver_bar_plot_data_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_device_semantic_quality_warnings_bar_plot_data"
        )

        silver_quality_metrics = SilverEventSemanticQualityMetrics(
            self.spark,
            input_silver_quality_metrics_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        silver_log_table = SilverEventSemanticQualityWarningsLogTable(
            self.spark, output_silver_log_table_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        silver_bar_plot_data = SilverEventSemanticQualityWarningsBarPlotData(
            self.spark, output_silver_bar_plot_data_path
        )

        self.input_data_objects = {silver_quality_metrics.ID: silver_quality_metrics}
        self.output_data_objects = {
            silver_log_table.ID: silver_log_table,
            silver_bar_plot_data.ID: silver_bar_plot_data,
        }

    def transform(self):
        # Pushup filter, select only dates needed
        # Since currently each QW has a different lookback period, we filter up to the
        # furthest day in the past needed

        metrics_df = self.input_data_objects[SilverEventSemanticQualityMetrics.ID].df

        furthest_lookback = max(self.thresholds[key]["sd_lookback_days"] for key in self.thresholds.keys())

        metrics_df = metrics_df.withColumn(
            "date", F.make_date(year=F.col(ColNames.year), month=F.col(ColNames.month), day=F.col(ColNames.day))
        ).filter(
            F.col("date").between(self.date_of_study - datetime.timedelta(days=furthest_lookback), self.date_of_study)
        )

        # Get all necessary metrics
        error_counts = metrics_df.select(["date", ColNames.type_of_error, ColNames.value]).collect()

        error_counts = [row.asDict() for row in error_counts]

        error_stats = dict()
        for count in error_counts:
            date = count["date"]
            if date not in error_stats:
                error_stats[date] = dict()

            error_stats[date][count[ColNames.type_of_error]] = count[ColNames.value]

        # If study date not present in the data, throw an exception
        if self.date_of_study not in error_stats.keys():
            raise ValueError(
                f"The date of study, {self.date_of_study.strftime(self.date_format)}, has no semantic checks metrics!"
            )

        for key in error_stats.keys():
            error_stats[key] = {"count": error_stats[key]}
            error_stats[key]["total"] = sum(error_stats[key]["count"].values())
            error_stats[key]["percentage"] = {
                type_of_error: 100 * val / error_stats[key]["total"]
                for type_of_error, val in error_stats[key]["count"].items()
            }

        for error_name in self.thresholds.keys():
            self.quality_warnings_by_error(error_name, error_stats)

        self.set_output_log_table()

        self.create_plots_data(error_stats)

    def quality_warnings_by_error(self, error_name: str, error_stats: dict):
        """Method that generates the quality warnings that will be recorded in the output log table,
        for each type of error.

        In the case that the data needed for a specific error's lookback period is not present, only the current date's
        error percentage is computed and no warning is raised.

        Args:
            error_name (str): name of the error, an attribute of multimno.core.constants.error_types.SemanticErrorType
            error_stats (dict): contains different values concerning each type of error, its counts, percentages, etc.
        """
        # Get the code of the error given its name
        error_code = getattr(SemanticErrorType, error_name)

        # lookback days for this error
        lookback_span = self.thresholds[error_name]["sd_lookback_days"]
        lookback_dates = [self.date_of_study - datetime.timedelta(days=dd) for dd in range(1, lookback_span + 1)]

        if not all(lookback_date in error_stats.keys() for lookback_date in lookback_dates):
            # cannot compute lookback mean and average, so only showing this date's percentages
            self.register_warning(
                date=self.date_of_study,
                error_code=error_code,
                value=error_stats[self.date_of_study]["percentage"][error_code],
                upper_control_limit=None,
                display_warning=False,
            )
        else:
            if lookback_span < self.MINIMUM_STD_LOOKBACK_DAYS:
                upper_control_limit = self.thresholds[error_name]["min_percentage"]
                self.logger.info(
                    f"Lookback days for {error_name} lower than {self.MINIMUM_STD_LOOKBACK_DAYS} - using fixed "
                    f"threshold {upper_control_limit}% instead of using average and standard deviation"
                )
            else:
                previous_avg = sum(error_stats[dd]["percentage"][error_code] for dd in lookback_dates) / lookback_span
                previous_std = math.sqrt(
                    sum((error_stats[dd]["percentage"][error_code] - previous_avg) ** 2 for dd in lookback_dates)
                    / (lookback_span - 1)
                )

                upper_control_limit = previous_avg + self.thresholds[error_name]["min_sd"] * previous_std

            # Now compare with todays value
            if error_stats[self.date_of_study]["percentage"][error_code] > upper_control_limit:
                display_warning = True
            else:
                display_warning = False

            self.register_warning(
                date=self.date_of_study,
                error_code=error_code,
                value=error_stats[self.date_of_study]["percentage"][error_code],
                upper_control_limit=upper_control_limit,
                display_warning=display_warning,
            )

    def register_warning(
        self, date: datetime.date, error_code: int, value: float, upper_control_limit: float, display_warning: bool
    ):
        """Method that abstracts away the creation in the correct format and data type, each of the quality
        warnings that will be recorded in the log table.

        Args:
            date (datetime.date): study date, for which the warnings are being calculated
            error_code (int): code of the error
            value (float): observed percentage of this specific error for the study date
            upper_control_limit (float): upper control limit, used as threshold for the warning
            display_warning (bool): whether the warning should be raised or not. It is currently independent of
                the arguments values, but in theory it should be equal to (value > control_limit)
        """
        self.warning_long_format.append((date, error_code, value, upper_control_limit, display_warning))

    def set_output_log_table(self):
        """
        Method that formats the warnings into the expected table format
        """
        warning_logs = pd.DataFrame(
            self.warning_long_format, columns=[ColNames.date, ColNames.type_of_error, "value", "UCL", "display"]
        ).pivot(index=ColNames.date, columns=[ColNames.type_of_error])
        column_names = []
        for name, code in warning_logs.columns:
            if name == "value":
                column_names.append(f"Error {code}")
            elif name == "UCL":
                column_names.append(f"Error {code} upper control limit")
            elif name == "display":
                column_names.append(f"Error {code} display warning")
        warning_logs.columns = column_names
        warning_logs = warning_logs.assign(execution_id=self.timestamp)
        warning_logs = warning_logs.reset_index().assign(
            **{
                ColNames.year: lambda df_: df_["date"].apply(lambda x: x.year),
                ColNames.month: lambda df_: df_["date"].apply(lambda x: x.month),
                ColNames.day: lambda df_: df_["date"].apply(lambda x: x.day),
            }
        )

        # Force expected order of columns
        warning_logs = warning_logs[SilverEventSemanticQualityWarningsLogTable.SCHEMA.names]

        log_table_df = self.spark.createDataFrame(
            warning_logs, schema=SilverEventSemanticQualityWarningsLogTable.SCHEMA
        )

        self.output_data_objects[SilverEventSemanticQualityWarningsLogTable.ID].df = log_table_df

    def create_plots_data(self, error_stats):
        """
        Method that takes the data needed to generate the required plots, and formats it for
        easier plotting and saving later on.
        """
        plot_data = []

        def format_error_code(code):
            if code == SemanticErrorType.NO_ERROR:
                return "No Error"

            return f"Error {code}"

        for date in error_stats:
            for error_code in error_stats[date]["count"]:
                plot_data.append(
                    Row(
                        **{
                            ColNames.date: date,
                            ColNames.type_of_error: format_error_code(error_code),
                            ColNames.value: float(error_stats[date]["count"][error_code]),
                            ColNames.variable: "Number of occurrences",
                            ColNames.year: self.date_of_study.year,
                            ColNames.month: self.date_of_study.month,
                            ColNames.day: self.date_of_study.day,
                            ColNames.timestamp: self.timestamp,
                        }
                    )
                )
            for error_code in error_stats[date]["percentage"]:
                plot_data.append(
                    Row(
                        **{
                            ColNames.date: date,
                            ColNames.type_of_error: format_error_code(error_code),
                            ColNames.value: float(error_stats[date]["percentage"][error_code]),
                            ColNames.variable: "Percentage",
                            ColNames.year: self.date_of_study.year,
                            ColNames.month: self.date_of_study.month,
                            ColNames.day: self.date_of_study.day,
                            ColNames.timestamp: self.timestamp,
                        }
                    )
                )
        self.output_data_objects[SilverEventSemanticQualityWarningsBarPlotData.ID].df = self.spark.createDataFrame(
            plot_data, schema=SilverEventSemanticQualityWarningsBarPlotData.SCHEMA
        )

    def write(self):
        # Write regular data objects
        self.logger.info("Writing data objects...")
        super().write()
        self.logger.info("Data objects written.")
