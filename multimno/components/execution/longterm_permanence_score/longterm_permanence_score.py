"""
Module that computes the Long-term Permanence Score
"""

from typing import List
import datetime as dt
import calendar as cal
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType, IntegerType
import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_midterm_permanence_score_data_object import (
    SilverMidtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import TIME_INTERVALS, DAY_TYPES, SEASONS
from multimno.core.log import get_execution_stats


class LongtermPermanenceScore(Component):
    """
    Class that computes the long term permanence score and related metrics, for different combinations of
    seasons, day types and time intervals.
    """

    COMPONENT_ID = "LongtermPermanenceScore"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        # Months that compose the long-term period, at least one
        start_month = self.config.get(self.COMPONENT_ID, "start_month")
        try:
            self.start_date = dt.datetime.strptime(start_month, "%Y-%m").date()
        except ValueError as e:
            self.logger.error(f"Could not parse start_month = `{start_month}`. Expected format: YYYY-MM")
            raise e

        end_month = self.config.get(self.COMPONENT_ID, "end_month")
        try:
            self.end_date = dt.datetime.strptime(end_month, "%Y-%m").date()
        except ValueError as e:
            self.logger.error(f"Could not parse end_month = `{end_month}`. Expected format: YYYY-MM")
            raise e
        self.end_date = self.end_date.replace(day=cal.monthrange(self.end_date.year, self.end_date.month)[1])

        if self.end_date < self.start_date:
            raise ValueError(f"End month `{end_month}` should not be an earlier than start month `{start_month}`")

        # Get months of the year in each season
        self.winter_months = self._get_month_list(self.config.get(self.COMPONENT_ID, "winter_months"), "winter")
        self.spring_months = self._get_month_list(self.config.get(self.COMPONENT_ID, "spring_months"), "spring")
        self.summer_months = self._get_month_list(self.config.get(self.COMPONENT_ID, "summer_months"), "summer")
        self.autumn_months = self._get_month_list(self.config.get(self.COMPONENT_ID, "autumn_months"), "autumn")

        # Check for possible repeated months
        all_season_months = self.winter_months + self.spring_months + self.summer_months + self.autumn_months
        if len(all_season_months) != len(set(all_season_months)):
            raise ValueError("at least one month belongs to more than one season -- please correct input parameters")

        # Read all subyearly-submonthly-subdaily combinations
        period_combinations = self.config.geteval(self.COMPONENT_ID, "period_combinations")
        self.period_combinations = {}
        for season, day_types_dict in period_combinations.items():
            if season.lower() not in SEASONS:
                raise ValueError(f"Unknown season `{season}` in period_combinations")
            self.period_combinations[season.lower()] = {}
            for day_type, time_intervals in day_types_dict.items():
                if day_type.lower() not in DAY_TYPES:
                    raise ValueError(f"Uknown day_type `{day_type}` under `{season}` in period_combinations")
                self.period_combinations[season.lower()][day_type.lower()] = []
                if len(time_intervals) != len(set(time_intervals)):
                    raise ValueError(
                        f"Repeated values for time_interval in under `{season}` and `{day_type}`:",
                        str(period_combinations[season][day_type]),
                    )
                for time_interval in time_intervals:
                    if time_interval not in TIME_INTERVALS:
                        raise ValueError(f"Unknown time_interval `{time_interval}` under `{season}` and `{day_type}`")
                    self.period_combinations[season.lower()][day_type.lower()].append(time_interval)

        # Check that all seasons for which an analysis is to be performed have been assigned at least one month
        for season, periods in self.period_combinations.items():
            if season == "all":
                continue
            if len(periods) > 0 and len(getattr(self, f"{season}_months")) == 0:
                raise ValueError(
                    f"Some period combinations have been requested for season `{season}` but no "
                    "month has been included in this season -- please correct the configuration "
                    "parameters"
                )

        # Get list of all the months that will be used, together with the seasons they belong to
        self.longterm_months = []
        self.season_months = {}

        for season in self.period_combinations:
            self.season_months[season] = []

        month_start_date = self.start_date
        while month_start_date < self.end_date:
            self.longterm_months.append(month_start_date)
            if "all" in self.season_months:
                self.season_months["all"].append(month_start_date)
            if "winter" in self.season_months and month_start_date.month in self.winter_months:
                self.season_months["winter"].append(month_start_date)
            if "spring" in self.season_months and month_start_date.month in self.spring_months:
                self.season_months["spring"].append(month_start_date)
            if "summer" in self.season_months and month_start_date.month in self.summer_months:
                self.season_months["summer"].append(month_start_date)
            if "autumn" in self.season_months and month_start_date.month in self.autumn_months:
                self.season_months["autumn"].append(month_start_date)

            month_start_date += dt.timedelta(days=cal.monthrange(month_start_date.year, month_start_date.month)[1])

        # Initialise variables for working with each longterm analysis
        self.current_lt_analysis = None
        self.longterm_analyses = None

    def _get_month_list(self, months_input: str, context: str) -> List[int]:
        """Read and parse a comma-separated list of months that will be assigned to a particular season. Months are
        represented by an integer from 1 to 12 and must not be repeated within the list


        Args:
            months_input (str): comma-separated list of months to be parsed
            context (str): name of the season, used for error tracking

        Raises:
            e: could not parse month to integer
            ValueError: integer is not one between 1 and 12, inclusive
            ValueError: repeated integers

        Returns:
            List[int]: list of integers representing the months of the year that will constitute a season
        """
        months_input = months_input.replace(" ", "").replace("\t", "")
        if months_input == "":
            return []

        months_input = months_input.split(",")
        months = []

        for mm in months_input:
            try:
                mm = int(mm)
            except ValueError as e:
                self.logger.error(f"expected integer as a month for {context} season, but found `{mm}`")
                raise e
            if mm < 1 or mm > 12:
                raise ValueError(
                    f"expected integer between 1 and 12 to represent a month for {context} season, " f"but found `{mm}`"
                )
            months.append(mm)

        if len(months) != len(set(months)):
            raise ValueError(f"found repeated month {months} in season {context} -- please remove any duplicates")

        return months

    def initalize_data_objects(self):
        input_silver_midterm_ps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "midterm_permanence_score_data_silver")
        output_silver_longterm_ps_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "longterm_permanence_score_data_silver"
        )

        midterm_ps = SilverMidtermPermanenceScoreDataObject(self.spark, input_silver_midterm_ps_path)
        longterm_ps = SilverLongtermPermanenceScoreDataObject(self.spark, output_silver_longterm_ps_path)

        self.input_data_objects = {midterm_ps.ID: midterm_ps}

        self.output_data_objects = {longterm_ps.ID: longterm_ps}

    def _check_midterm_data_exist(self) -> List[dict]:
        """Checks that the mid-term permanence score data necessary for carrying out all long-term analyses exist,
        based on the day_types and time_intervals requested for each analysis. Returns a list of dictionaries
        containing the information necessary to filter the required mid-term analysis data of each analysis.

        Raises:
            FileNotFoundError: Whenever a certain (day_type, time_interval) combination has not been found in the
                data of a particular month, required to compute some long-term analysis.

        Returns:
            List[dict]: information of each long-term analysis to be performed
        """
        midterm_df = self.input_data_objects[SilverMidtermPermanenceScoreDataObject.ID].df
        longterm_analyses = []
        already_checked = set()
        # For each season
        for season, months in self.season_months.items():
            # Check that all months of that season have the mid-term PS data of the required day type and time interval
            for day_type, time_intervals in self.period_combinations[season].items():
                for time_interval in time_intervals:
                    for month in months:
                        if (month, day_type, time_interval) in already_checked:
                            continue

                        partition_filter = (
                            (F.col(ColNames.year) == F.lit(month.year))
                            & (F.col(ColNames.month) == F.lit(month.month))
                            & (F.col(ColNames.day_type) == F.lit(day_type))
                            & (F.col(ColNames.time_interval) == F.lit(time_interval))
                        )

                        data_exists = midterm_df.where(partition_filter).limit(1).count() > 0
                        if not data_exists:
                            raise FileNotFoundError(
                                "No Mid-term Permanence Score data has been found for month "
                                f"{month.strftime('%Y-%m')}, day_type `{day_type}` and time_interval `{time_interval}`"
                            )
                        already_checked.add((month, day_type, time_interval))

                    longterm_analyses.append(
                        {
                            "season": season,
                            "months": months,
                            "day_type": day_type,
                            "time_interval": time_interval,
                        }
                    )

        return longterm_analyses

    def filter_longterm_analysis_data(self, df: DataFrame) -> DataFrame:
        """Applies the push-up filters on the Mid-term Permanence Score data to only consider the data from the
        months, day_type, and time_interval required for the current long-term analysis combination.

        Args:
            df (DataFrame): Mid-term Permanence Score DataFrame before filtering

        Returns:
            DataFrame: Mid-term Permanence Score DataFrame after filtering
        """
        month_filters = [
            (F.col(ColNames.year) == F.lit(month.year)) & (F.col(ColNames.month) == F.lit(month.month))
            for month in self.current_lt_analysis["months"]
        ]

        def logical_or(x, y):
            return x | y

        month_filter = reduce(logical_or, month_filters)

        day_type_filter = F.col(ColNames.day_type) == F.lit(self.current_lt_analysis["day_type"])

        time_interval_filter = F.col(ColNames.time_interval) == F.lit(self.current_lt_analysis["time_interval"])

        filtered_df = df.filter(month_filter & day_type_filter & time_interval_filter)

        return filtered_df

    def compute_longterm_metrics(self, df: DataFrame) -> DataFrame:
        """Compute the Long-term Permanence Score and metrics from the filtered Mid-term Permanence Score data

        Args:
            df (DataFrame): Mid-term Permanence Score DataFrame

        Returns:
            DataFrame: dataframe with the long-term permanence score aend metrics
        """
        # Split dataframes by id_type
        grid_df = df.filter(F.col(ColNames.id_type) == F.lit("grid"))
        unknown_df = df.filter(F.col(ColNames.id_type) == F.lit("unknown"))
        obs_df = df.filter(F.col(ColNames.id_type) == F.lit("device_observation"))

        grid_df = grid_df.groupby(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id).agg(
            F.sum(ColNames.mps).cast(IntegerType()).alias(ColNames.lps),
            F.sum(ColNames.frequency).cast(IntegerType()).alias(ColNames.total_frequency),
            F.mean(ColNames.frequency).cast(FloatType()).alias(ColNames.frequency_mean),
            F.stddev_samp(ColNames.frequency).cast(FloatType()).alias(ColNames.frequency_std),
            F.mean(ColNames.regularity_mean).cast(FloatType()).alias(ColNames.regularity_mean),
            F.stddev_samp(ColNames.regularity_mean).cast(FloatType()).alias(ColNames.regularity_std),
        )

        unknown_df = unknown_df.groupby(ColNames.user_id_modulo, ColNames.user_id).agg(
            F.sum(ColNames.mps).cast(IntegerType()).alias(ColNames.lps),
            F.sum(ColNames.frequency).cast(IntegerType()).alias(ColNames.total_frequency),
            F.mean(ColNames.frequency).cast(FloatType()).alias(ColNames.frequency_mean),
            F.stddev_samp(ColNames.frequency).cast(FloatType()).alias(ColNames.frequency_std),
            F.mean(ColNames.regularity_mean).cast(FloatType()).alias(ColNames.regularity_mean),
            F.stddev_samp(ColNames.regularity_mean).cast(FloatType()).alias(ColNames.regularity_std),
        )

        obs_df = obs_df.groupby(ColNames.user_id_modulo, ColNames.user_id).agg(
            F.sum(ColNames.mps).cast(IntegerType()).alias(ColNames.lps),
            F.sum(ColNames.frequency).cast(IntegerType()).alias(ColNames.total_frequency),
        )

        grid_df = grid_df.select(
            ColNames.user_id_modulo,
            ColNames.user_id,
            ColNames.grid_id,
            ColNames.lps,
            ColNames.total_frequency,
            ColNames.frequency_mean,
            ColNames.frequency_std,
            ColNames.regularity_mean,
            ColNames.regularity_std,
            F.lit("grid").alias(ColNames.id_type),
        )

        unknown_df = unknown_df.select(
            ColNames.user_id_modulo,
            ColNames.user_id,
            F.lit("unknown").alias(ColNames.grid_id),
            ColNames.lps,
            ColNames.total_frequency,
            ColNames.frequency_mean,
            ColNames.frequency_std,
            ColNames.regularity_mean,
            ColNames.regularity_std,
            F.lit("unknown").alias(ColNames.id_type),
        )

        obs_df = obs_df.select(
            ColNames.user_id_modulo,
            ColNames.user_id,
            F.lit("device_observation").alias(ColNames.grid_id),
            ColNames.lps,
            ColNames.total_frequency,
            F.lit(None).cast(FloatType()).alias(ColNames.frequency_mean),
            F.lit(None).cast(FloatType()).alias(ColNames.frequency_std),
            F.lit(None).cast(FloatType()).alias(ColNames.regularity_mean),
            F.lit(None).cast(FloatType()).alias(ColNames.regularity_std),
            F.lit("device_observation").alias(ColNames.id_type),
        )

        return grid_df.union(unknown_df).union(obs_df)

    def transform(self):
        midterm_df = self.input_data_objects[SilverMidtermPermanenceScoreDataObject.ID].df

        df = self.filter_longterm_analysis_data(midterm_df)

        longterm_df = self.compute_longterm_metrics(df)

        start_date = min(self.current_lt_analysis["months"])
        end_date = max(self.current_lt_analysis["months"])
        end_date = end_date.replace(day=cal.monthrange(end_date.year, end_date.month)[1])

        longterm_df = longterm_df.withColumns(
            {
                ColNames.start_date: F.lit(start_date),
                ColNames.end_date: F.lit(end_date),
                ColNames.season: F.lit(self.current_lt_analysis["season"]),
                ColNames.day_type: F.lit(self.current_lt_analysis["day_type"]),
                ColNames.time_interval: F.lit(self.current_lt_analysis["time_interval"]),
            }
        )

        longterm_df = longterm_df.repartition(*SilverLongtermPermanenceScoreDataObject.PARTITION_COLUMNS)

        self.output_data_objects[SilverLongtermPermanenceScoreDataObject.ID].df = longterm_df

    @get_execution_stats
    def execute(self):
        self.logger.info("Reading data objects...")
        self.read()
        self.logger.info("... data objects read!")
        self.logger.info("Checking that all required mid-term permanence score data exists...")
        self.longterm_analyses = self._check_midterm_data_exist()
        self.logger.info("... check successful!")
        self.logger.info("Starting long-term analyses...")
        for lt_analysis in self.longterm_analyses:
            self.current_lt_analysis = lt_analysis
            self.logger.info(
                f"Starting analysis for season `{self.current_lt_analysis['season']}`, "
                f"{min(self.current_lt_analysis['months']).strftime('%Y-%m')} to "
                f"{max(self.current_lt_analysis['months']).strftime('%Y-%m')}, "
                f"day_type `{self.current_lt_analysis['day_type']}` and "
                f"time_interval `{self.current_lt_analysis['time_interval']}`..."
            )
            self.transform()
            self.write()
            self.logger.info("... results saved")

        self.logger.info("... all analyses finished!")
