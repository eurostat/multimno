"""
Module that computes the Mid-term Permanence Score.
"""

import datetime as dt
import calendar as cal
import logging
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, ArrayType, FloatType, IntegerType, ShortType, ByteType
import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_holiday_calendar_data_object import BronzeHolidayCalendarDataObject
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_midterm_permanence_score_data_object import (
    SilverMidtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import TIME_INTERVALS, DAY_TYPES
from multimno.core.log import get_execution_stats


@F.udf(returnType=ArrayType(FloatType()))
def frequency_and_regularity(
    arr, month_start: dt.date, extended_start: dt.date, month_end: dt.date, extended_end: dt.date
) -> List[float]:
    """PySpark UDF that calculates the mid-term frequency and regularity metrics of a device and grid tile/unknown
    location

    Args:
        arr: ArrayType() PySpark SQL Type (seen as a list?) containing the dates in which the device has been observed
            in a particular grid tile or in the "unknown" location. All dates belong to the mid-term's month of
            analysis, with the possible exception of a date in the look-back dates and a date in the look-forward
            dates used for the regularity indices' calculation.
        month_start (dt.date): First date of the month of the current mid-term period
        extended_start (dt.date): First date of the extended dates in the look-back dates
        month_end (dt.date): Last date of the month of the current mid-term period
        extended_end (dt.date): Last date of the extended dates in the look-forward dates

    Raises:
        ValueError: Whenever a record reaches this function with an empty list of dates, which is unexpected behaviour

    Returns:
        List[float]: returns a list with the frequency, regularity mean and regularity sample standard deviation
    """
    if len(arr) == 0:
        raise ValueError("Found empty array of dates during regularity mean calculation. which should not be possible")

    earliest_date = arr[0]
    latest_date = arr[-1]

    array_length = len(arr)

    # this variable represents the number of dates in the current month that have been observed. since some dates in
    # `arr` might belong to the look-back or look-forward period, this value might be adjusted later on
    frequency = array_length

    # if there is only one date in the array (so earliest_date equals latest_date)
    if array_length == 1:
        # if the single date does not belong to the study month, there is no stay in this grid tile and month
        if earliest_date < month_start or earliest_date > month_end:
            return None

        return (
            1.0,
            (extended_end - extended_start).days / (array_length + 1),
            (extended_end - extended_start).days / (2**0.5),
        )
    # if there are only two dates
    elif array_length == 2:
        # If the dates belong to the reg extended periods, there is no stay in this grid tile and month
        if earliest_date < month_start or earliest_date > month_end:
            return None

    diffs = [(arr[i + 1] - arr[i]).days for i in range(len(arr) - 1)]
    # If there is no date in the before (after) reg period, we consider the start (end) date as the extended_start
    # (extended_end) date.
    if earliest_date >= month_start:
        diffs.append((earliest_date - extended_start).days)
        earliest_date = extended_start
        array_length += 1
    else:
        frequency -= 1
    if latest_date <= month_end:
        diffs.append((extended_end - latest_date).days)
        latest_date = extended_end
        array_length += 1
    else:
        frequency -= 1

    mean = (latest_date - earliest_date).days / (array_length - 1)
    std = (sum((dd - mean) ** 2 for dd in diffs) / (array_length - 2)) ** 0.5
    return [float(frequency), mean, std]


class MidtermPermanenceScore(Component):
    """
    Class that computes the mid term permanence score and related metrics, for different
    combinations of day types and time intervals in the day
    """

    COMPONENT_ID = "MidtermPermanenceScore"

    night_time_start, night_time_end = None, None
    working_hours_start, working_hours_end = None, None
    evening_time_start, evening_time_end = None, None

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        # Months to process as each mid-term period
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

        if end_month < start_month:
            raise ValueError(f"End month `{end_month}` should not be an earlier than start month `{start_month}`")

        # Additional days before and after each month to use for calculating regularity metrics
        self.before_reg_days = self.config.getint(self.COMPONENT_ID, "before_regularity_days")
        if self.before_reg_days < 0:
            raise ValueError(f"`before_reg_days` must be a non-negative integer, found {self.before_reg_days}")

        self.after_reg_days = self.config.getint(self.COMPONENT_ID, "after_regularity_days")
        if self.after_reg_days < 0:
            raise ValueError(f"`after_reg_days` must be a non-negative integer, found {self.after_reg_days}")

        # list of dictionaries with each mid-term to be analysed
        self.midterm_periods = self._get_midterm_periods()
        # Hour used to define the start of a day, e.g. 4 means that a Monday starts at 4AM Monday and ends at
        # 4AM Tuesday
        self.day_start_hour = self.config.getint(self.COMPONENT_ID, "day_start_hour")
        if self.day_start_hour < 0 or self.day_start_hour >= 24:
            raise ValueError(f"`day_start_hour` must be between 0 and 23 inclusive, found {self.day_start_hour} ")

        # Read the definition of each sub-daily period (or time interval) to be studied.
        # Set to keep track of the minutes in each the intervals start/end, which must be compared with
        # daily permanence score to verify compatibility
        self.midterm_minutes = set()
        for time_interval in TIME_INTERVALS:  # night, work, evening, all
            if time_interval == "all":
                continue
            interval_start = self.config.get(self.COMPONENT_ID, f"{time_interval}_start")
            interval_start = self._check_time_interval(interval_start, name=f"{time_interval}_start")
            setattr(self, f"{time_interval}_start", interval_start)

            interval_end = self.config.get(self.COMPONENT_ID, f"{time_interval}_end")
            interval_end = self._check_time_interval(interval_end, name=f"{time_interval}_end")
            setattr(self, f"{time_interval}_end", interval_end)

            if interval_start == interval_end:
                raise ValueError(
                    f"{time_interval}_start and {time_interval}_end are equal, when they must be strictly "
                    "different -- please provide a valid time interval"
                )

            # Non-allowed time interval limits. Example:
            # self.day_start_hour = 4 (4AM)
            # interval_start = 03:30, interval_end = 01:00
            # The time interval starts at 03:30 of day D-1 and ends at 01:00 of day D, but would belong to day D-1
            if (
                interval_start.hour < self.day_start_hour
                and interval_end != dt.time(0, 0)
                and (interval_end < interval_start)
            ):
                raise ValueError(
                    "Invalid configuration: the following order of of parameters is not allowed:\n"
                    f"\t {time_interval}_end ({interval_end}) < {time_interval}_start ({interval_start}) < "
                    f"day_start_hour ({self.day_start_hour})"
                )

            # Additional prohibited time interval (except for nights): time interval must not cross the self.dat_start_hour
            if time_interval != "night_time":
                if interval_start.hour < self.day_start_hour and (
                    interval_end.hour > self.day_start_hour
                    or (interval_end.hour == self.day_start_hour and interval_end.minute != 0)
                ):
                    raise ValueError(
                        "Invalid configuration: the following order of parameters is not allowed:\n"
                        f"{time_interval}_start ({interval_start}) < day_start_hour ({self.day_start_hour}) < {time_interval}_end ({interval_end})"
                    )

            self.midterm_minutes.add(interval_start.minute)
            self.midterm_minutes.add(interval_end.minute)

        # Day of the week marking the start of the weekend, (starting in self.day_start_hour)
        weekend_start_str = self.config.get(self.COMPONENT_ID, "weekend_start")
        self.weekend_start_day = self._check_weekday_number(weekend_start_str, context=weekend_start_str)

        # Day of the week marking the end of the weekend, date included, (ending right before self.day_start_hour)
        weekend_end_str = self.config.get(self.COMPONENT_ID, "weekend_end")
        self.weekend_end_day = self._check_weekday_number(weekend_end_str, context=weekend_end_str)

        # List of days of the week composing the weekend
        self.weekend_days = []
        dd = self.weekend_start_day
        while dd != self.weekend_end_day:
            self.weekend_days.append(dd)
            dd = (dd) % 7 + 1
        self.weekend_days.append(dd)

        # Work days are those that are not part of the weekend (also excluding holidays later on)
        self.work_days = sorted(list({1, 2, 3, 4, 5, 6, 7}.difference(self.weekend_days)))

        # Read from configuration the combination of sub-monthly and sub-daily pairs, i.e. day types and time intervals,
        # to compute
        period_combinations = self.config.geteval(self.COMPONENT_ID, "period_combinations")
        self.period_combinations = {}
        for key, vals in period_combinations.items():
            if key.lower() not in DAY_TYPES:
                raise ValueError(f"Unknown day type `{key}` in period_combinations")
            self.period_combinations[key.lower()] = []
            if len(vals) != len(set(vals)):
                raise ValueError(
                    f"Repeated values for time interval in period_combinations under `{key}`:",
                    str(period_combinations[key]),
                )
            for val in vals:
                if val not in TIME_INTERVALS:
                    raise ValueError(f"Unknown time interval `{val}` in period_combinations under `{key}`")
                self.period_combinations[key.lower()].append(val)

        # Score interval, for DPS calculation, currently set = 2
        self.score_interval = 2

        # Country of study, used to load its holidays
        self.country_of_study = self.config.get(self.COMPONENT_ID, "country_of_study")

        # Initialise variable for working in each midterm_period
        self.day_type = None
        self.time_interval = None
        self.current_mt_period = None
        self.current_dps_data = None
        self.footprint = None

    def _get_midterm_periods(self) -> List[dict]:
        """Computes the date limits of each mid-term period, together with the limits of the regularity metrics' extra
        dates

        Returns:
            List[dict]: list of dictionaries with the information of dates of each mid-term period
        """
        midterm_periods = []
        start_of_the_month = self.start_date

        while True:
            end_of_the_month = start_of_the_month.replace(
                day=cal.monthrange(start_of_the_month.year, start_of_the_month.month)[1]
            )
            before_reg_date = start_of_the_month - dt.timedelta(days=self.before_reg_days)
            after_reg_date = end_of_the_month + dt.timedelta(days=self.after_reg_days)

            midterm_periods.append(
                {
                    "month_start": start_of_the_month,
                    "month_end": end_of_the_month,
                    "extended_month_start": before_reg_date,
                    "extended_month_end": after_reg_date,
                }
            )

            if end_of_the_month == self.end_date:
                return midterm_periods

            start_of_the_month = end_of_the_month + dt.timedelta(days=1)

    def _check_weekday_number(self, num: str, context: str) -> int:
        """Parses and validates a day of the week

        Args:
            num (str): string to be parsed to integer between 1 and 7
            context (str): string for error tracking

        Raises:
            e: Error in parsing num to int
            ValueError: num is not a valid day of the week (between 1 and 7 inclusive)

        Returns:
            int: integer representing a day of the week
        """
        try:
            num = int(num)
        except Exception as e:
            self.logger.error(f"Must specify a day as an integer between 1 and 7, but found `{num}` in `{context}`")
            raise e
        if num < 1 or num > 7:
            raise ValueError(
                f"Days must take a value between 1 for Monday and 7 for Sunday, found {num} in `{context}`"
            )
        return num

    def _check_time_interval(self, interval: str, name: str) -> dt.time:
        """Tries to parse time interval's start/end time from configuration file and check if it has
        valid minutes (00, 15, 30, or 45). If so, returns the corresponding dt.time object

        Args:
            interval (str): interval string to be parsed to dt.datetime
            name (str): name of the interval being parsed, used for error tracking

        Raises:
            e: Formatting error, cannot parse time as HH:MM (24h format)
            ValueError: interval ends in non-allowed minutes

        Returns:
            dt.time: time of the start or end of the time interval
        """
        try:
            interval = dt.datetime.strptime(interval, "%H:%M")
        except ValueError as e:
            self.logger.error(f"Could not parse {name}, expected HH:MM format, found {interval}")
            raise e

        interval = interval.time()
        if interval.minute not in [0, 15, 30, 45]:
            raise ValueError(f"Time interval {name} must have :00, :15, :30, or :45 minutes, found :{interval.minute}")
        return interval

    def initalize_data_objects(self):
        input_silver_daily_ps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_data_silver")
        input_bronze_holiday_calendar_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "holiday_calendar_data_bronze")
        input_silver_cell_footprint = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        output_silver_midterm_ps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "midterm_permanence_score_data_silver")

        daily_ps = SilverDailyPermanenceScoreDataObject(self.spark, input_silver_daily_ps_path)
        holiday_calendar = BronzeHolidayCalendarDataObject(self.spark, input_bronze_holiday_calendar_path)
        cell_footprint = SilverCellFootprintDataObject(self.spark, input_silver_cell_footprint)
        midterm_ps = SilverMidtermPermanenceScoreDataObject(self.spark, output_silver_midterm_ps_path)

        self.input_data_objects = {
            holiday_calendar.ID: holiday_calendar,
            daily_ps.ID: daily_ps,
            cell_footprint.ID: cell_footprint,
        }
        self.output_data_objects = {midterm_ps.ID: midterm_ps}

    def _validate_and_load_daily_permanence_score(self, mt_period: dict) -> DataFrame:
        """Loads the Daily Permanence Score data to be used for the calculation of the Mid-term Permanence Score metrics
        of a particular mid-term period. Filters out DPS values equal to zero and checks that the time slots are
        compatible with the configuration-provided time intervals.

        Raises:
            ValueError: If DPS data has a time slot duration different from 15, 30, or 60 minutes.
            ValueError: If DPS data has 60-min slots but 15- or 30-min lengths are required.
            ValueError: If DPS data has 30-min slots but 15-min lengths are required.

        Returns:
            dps: DataFrame of all DPS data necessary to calcualte the mid-term permanence score & metrics of
                self.current_mt_period
        """
        dps = self.input_data_objects[SilverDailyPermanenceScoreDataObject.ID].df

        # First filter: take out stay_duration = 0  # TODO: filter out unknowns??
        dps = dps.filter(F.col(ColNames.stay_duration) > 0)

        # Add a one-day buffer, as later on the definition of a day does not match the midnight definition
        dps = dps.filter(
            F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
            >= F.lit(mt_period["extended_month_start"] - dt.timedelta(days=1))
        ).filter(
            F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
            <= F.lit(mt_period["extended_month_end"]) + dt.timedelta(days=1)
        )

        # If all time_intervals match a whole hour, no check needs to be done here.
        if self.midterm_minutes == {0}:
            return dps

        # If the previous set was not disjoint, then the user has entered some time_interval that does not match a
        # whole hour. We check, for all loaded dates, what their time slot length was. We only need to check the length
        # of one time slot per DPS date.
        time_slot_duration = (
            dps.groupby([ColNames.year, ColNames.month, ColNames.day])
            .agg(
                F.first(F.col(ColNames.time_slot_end_time) - F.col(ColNames.time_slot_initial_time)).alias(
                    "time_slot_duration"
                )
            )
            .collect()
        )
        daily_duration = {
            dt.date(row["year"], row["month"], row["day"]): row["time_slot_duration"].seconds // 60
            for row in time_slot_duration
        }

        # If disjoint, user has specified intervals ending in :00 or :30. Then we admit 15- or 30-min time slot
        # durations, but not whole hours.
        # If not disjoint, user has specified some :15 or :45 intervals, and we can only admit 15-min time slots
        check_for_half_hour_only = {15, 45}.isdisjoint(self.midterm_minutes)

        for date, duration in daily_duration.items():
            if duration not in {15, 30, 60}:
                raise ValueError(
                    f"Found time_slot duration of {duration} min in DailyPermanenceScore of {date}, when "
                    "accepted values are 15, 30, or 60 minutes."
                )
            if duration == 60:
                if check_for_half_hour_only:
                    msg = (
                        f"Found time_slot duration of 60 min in DailyPermanenceScore of {date}, when only accepted "
                        "values are 15 and 30"
                    )
                else:
                    msg = (
                        f"Found time_slot duration of 60 min in DailyPermanenceScore of {date}, when only accepted "
                        "value is 15"
                    )
                raise ValueError(msg)
            if duration == 30 and not check_for_half_hour_only:
                raise ValueError(
                    f"Found time_slot duration of 30 min in DailyPermanenceScore of {date}, when only accepted "
                    "value is 15"
                )
        return dps

    def filter_dps_by_time_interval(
        self, df: DataFrame, subdaily_period: str, start: dt.time, end: dt.time
    ) -> DataFrame:
        """Filter DPS matrix by keeping the time slots that belong to the subdaily period or time interval
        specified. Also create a new column, `date`, which contains to which day the time slot belongs to, as it
        does not necessarily match the day the time slot belongs to (i.e. the `midnight` definition)

        Args:
            df (DataFrame): DPS dataframe to be filtered.
            subdaily_period (str): name of the time interval or subdaily period. Must be one of the values in
                MidtermPermanenceScore.TIME_INTERVALS.
            start (dt.time): earliest time of accepted time slots that will not be filtered out
            end (dt.time): latest time of accepted time slots that will not be filtered out

        Raises:
            ValueError: Whenever an unknown subdaily period is specified

        Returns:
            DataFrame: filtered DPS dataframe
        """
        if subdaily_period not in TIME_INTERVALS:
            raise ValueError(f"Unknown subdaily/time_interval {subdaily_period}")

        # Auxiliary variables
        start_hour = F.lit(start.hour)
        start_min = F.lit(start.minute)
        end_hour = end.hour
        if end_hour == 0:
            end_hour = F.lit(24)
        else:
            end_hour = F.lit(end_hour)
        end_min = F.lit(end.minute)

        slot_initial_hour = F.hour(ColNames.time_slot_initial_time)
        slot_initial_min = F.minute(ColNames.time_slot_initial_time)
        slot_end_hour = F.hour(ColNames.time_slot_end_time)
        slot_end_hour = F.when(slot_end_hour == F.lit(0), F.lit(24)).otherwise(slot_end_hour)
        slot_end_min = F.minute(ColNames.time_slot_end_time)

        # Global time interval, taking all time slots
        if subdaily_period == "all":
            if start != end:
                raise ValueError(
                    "`all` time interval must have matching start and end times to not overlap with "
                    f"different dates, found start={start} and end={end}"
                )
            # If the day starts and ends at midnight, no additional logic needs to be done for assigning the correct
            # date
            if start == dt.time(0, 0):
                df = df.withColumn(ColNames.date, F.col(ColNames.time_slot_initial_time).cast(DateType()))
                return df
            # If not: the hour that defines the day always belongs to that day.
            # e.g., if subdaily_start=4, then day D is defined as 4:00AM of day D to 4:00 AM of day D+1
            df = df.withColumn(
                ColNames.date,
                F.when(
                    F.lit(60) * (slot_initial_hour - start_hour) >= (start_min - slot_initial_min),
                    F.col(ColNames.time_slot_initial_time),
                )
                .otherwise(F.date_add(F.col(ColNames.time_slot_initial_time), -1))
                .cast(DateType()),
            )
            return df

        # Rest of time intervals: night_time, evening_time, working_hours.
        # Filter out time slots not contained in the time interval
        if start < end or end == dt.time(0, 0):
            df = df.filter(
                (F.lit(60) * (slot_initial_hour - start_hour) >= (start_min - slot_initial_min))
                & (F.lit(60) * (slot_end_hour - end_hour) <= (end_min - slot_end_min))
            )
        else:
            df = df.filter(
                (F.lit(60) * (slot_initial_hour - start_hour) >= (start_min - slot_initial_min))
                | (F.lit(60) * (slot_end_hour - end_hour) <= (end_min - slot_end_min))
            )

        # consider self.day_start_hour = 4 for the following examples
        if subdaily_period == "night_time":
            if start.hour >= self.day_start_hour and (end > start or end == dt.time(0, 0)):
                # night-interval stays in the day, example [20:15 to 23:30] or [19:45 to 00:00]
                df = df.withColumn(ColNames.date, F.col(ColNames.time_slot_initial_time).cast(DateType()))
            elif start.hour >= self.day_start_hour and end < start and end != dt.time(0, 0):
                # night-interval starts in the day and crosses midnight, example [20:15 to 03:30] or [19:00 to 18:45]
                df = df.withColumn(
                    ColNames.date,
                    F.when(
                        F.lit(60) * (slot_initial_hour - start_hour) >= (start_min - slot_initial_min),
                        F.col(ColNames.time_slot_initial_time),
                    )
                    .otherwise(F.date_add(F.col(ColNames.time_slot_initial_time), -1))
                    .cast(DateType()),
                )
            elif start.hour < self.day_start_hour and (end > start or end == dt.time(0, 0)):
                # night-interval starts in the next day and does not cross midnight, example [3:15 to 6:00] or [2:30 to 10:45]
                df = df.withColumn(
                    ColNames.date, F.date_add(F.col(ColNames.time_slot_initial_time), -1).cast(DateType())
                )
            return df

        # if subdaily_period in ("working_hours", "evening_time"):
        if start >= end and end != dt.time(0, 0):
            self.logger.log(
                msg=f"'inverted' start ({start}) and end ({end}) times found for subdaily period {subdaily_period}"
                " -- the whole period will belong to the day of start of the interval",
                level=logging.INFO,
            )
        df = df.withColumn(
            ColNames.date,
            F.when(
                F.lit(60) * (slot_initial_hour - start_hour) >= (start_min - slot_initial_min),
                F.col(ColNames.time_slot_initial_time),
            )
            .otherwise(F.date_add(F.col(ColNames.time_slot_initial_time), -1))
            .cast(DateType()),
        )
        return df

    def filter_dps_by_day_type(self, df: DataFrame, submonthly_period: str) -> DataFrame:
        """Filter DPS matrix, with an assigned date column, keeping the time slots that belong to the day type or
        submonthly period specified.

        Args:
            df (DataFrame): DPS dataframe, with assigned `date` column
            submonthly_period (str): submonthly period or day type. Must be one of the values in
                MidtermPermanenceScore.DAY_TYPE

        Raises:
            ValueError: Whenever an unknown submonthly period is specified

        Returns:
            DataFrame: Filtered dataframe
        """
        # Now that we have the date column, we filter unneeded dates again, as we took one extra day before and after
        df = df.filter(
            F.col(ColNames.date).between(
                lowerBound=self.current_mt_period["extended_month_start"],
                upperBound=self.current_mt_period["extended_month_end"],
            )
        )

        if submonthly_period not in DAY_TYPES:
            raise ValueError(f"Unknown submonthly period/day type `{submonthly_period}`")
        if submonthly_period == "all":
            return df

        if submonthly_period == "weekends":
            df = df.filter((F.weekday(ColNames.date) + F.lit(1)).isin(self.weekend_days))
            return df
        if submonthly_period == "mondays":
            df = df.filter((F.weekday(ColNames.date)).isin([0]))
            return df

        if submonthly_period == "tuesdays":
            df = df.filter((F.weekday(ColNames.date)).isin([1]))
            return df
        if submonthly_period == "wednesdays":
            df = df.filter((F.weekday(ColNames.date)).isin([2]))
            return df
        if submonthly_period == "thursdays":
            df = df.filter((F.weekday(ColNames.date)).isin([3]))
            return df
        if submonthly_period == "fridays":
            df = df.filter((F.weekday(ColNames.date)).isin([4]))
            return df
        if submonthly_period == "saturdays":
            df = df.filter((F.weekday(ColNames.date)).isin([5]))
            return df
        if submonthly_period == "sundays":
            df = df.filter((F.weekday(ColNames.date)).isin([6]))
            return df

        holidays = F.broadcast(self.input_data_objects["BronzeHolidayCalendarInfoDO"].df)
        holidays = holidays.filter(F.col(ColNames.iso2) == F.lit(self.country_of_study))
        if submonthly_period == "holidays":
            df = df.join(holidays.select(ColNames.date), on=ColNames.date, how="inner")
        # Workdays are all days falling in one of self.work_days and not being a holiday
        if submonthly_period == "workdays":
            df = df.join(holidays.select(ColNames.date), on=ColNames.date, how="left_anti").filter(
                (F.weekday(ColNames.date) + F.lit(1)).isin(self.work_days)
            )
        return df

    def compute_daily_permanence_score(self, df: DataFrame) -> DataFrame:
        """
        Compute daily permanence score at grid tile level from stay durations at cell level and
        cell footprint datasets.

        Args:
            df (DataFrame): dataset with stay durations at cell level.

        Returns:
            DataFrame: dps dataset at grid tile level.
        """
        """unknown = df.where(F.col(ColNames.id_type) == F.lit("unknown")).select(
            ColNames.date,
            ColNames.user_id_modulo,
            ColNames.user_id,
            F.lit("unknown").alias("grid_id"),
            ColNames.time_slot_initial_time,
            ColNames.stay_duration,
            F.lit("unknown").alias(ColNames.id_type),
        )"""

        dps = (
            df.join(self.footprint, on=[ColNames.year, ColNames.month, ColNames.day, ColNames.cell_id], how="inner")
            .groupby(
                ColNames.date,
                ColNames.user_id_modulo,
                ColNames.user_id,
                ColNames.grid_id,
                ColNames.time_slot_initial_time,
            )
            .agg(F.sum(ColNames.stay_duration).cast(FloatType()).alias(ColNames.stay_duration))
            .withColumn(ColNames.id_type, F.lit("grid"))
            # .union(unknown)
            .withColumn(
                ColNames.dps,
                F.lit(-1)
                + F.ceil(F.lit(self.score_interval) * F.col(ColNames.stay_duration) / F.lit(3600)).cast(ByteType()),
            )
            .filter(F.col(ColNames.dps) > 0)
            .drop(ColNames.stay_duration)
        )

        return dps

    def compute_midterm_metrics(self, df: DataFrame) -> DataFrame:
        """Compute the mid-term permanence score and metrics of the current mid-term period, submonthly (i.e. day type)
        and subdaily (i.e. time interval) combination.

        Args:
            df (DataFrame): filtered DPS DataFrame with added `date` column

        Returns:
            DataFrame: resulting DataFrame
        """
        # Find latest stay of the user in each grid tile during the lookback period for regularity metrics
        # (if it exists)
        before_reg = (
            df.filter(F.col(ColNames.date) < F.lit(self.current_mt_period["month_start"]))
            .groupBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)
            .agg(F.max(ColNames.date).alias(ColNames.date))
        )

        # Find earliest stay of the user in each grid tile during the lookback period for regularity metrics
        # (if it exists)
        after_reg = (
            df.filter(F.col(ColNames.date) > F.lit(self.current_mt_period["month_end"]))
            .groupBy(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)
            .agg(F.min(ColNames.date).alias(ColNames.date))
        )

        # Current month data
        study_df = df.filter(
            F.col(ColNames.date).between(
                lowerBound=self.current_mt_period["month_start"], upperBound=self.current_mt_period["month_end"]
            )
        )

        # Device observation metric
        observation_df = (
            study_df.filter(F.col(ColNames.id_type) == F.lit("grid"))
            .groupby(ColNames.user_id_modulo, ColNames.user_id, ColNames.date)
            .agg(F.count_distinct(ColNames.time_slot_initial_time).alias("observed_day_dps"))
            .groupby(ColNames.user_id_modulo, ColNames.user_id)
            .agg(
                F.sum("observed_day_dps").alias(ColNames.mps),
                F.count_distinct(ColNames.date).cast(IntegerType()).alias(ColNames.frequency),
            )
            .select(
                ColNames.user_id_modulo,
                ColNames.user_id,
                F.lit("device_observation").alias(ColNames.grid_id),
                F.col(ColNames.mps).cast(IntegerType()).alias(ColNames.mps),
                ColNames.frequency,
                F.lit(None).cast(FloatType()).alias(ColNames.regularity_mean),
                F.lit(None).cast(FloatType()).alias(ColNames.regularity_std),
                F.lit("device_observation").alias(ColNames.id_type),
            )
        )

        combined_df = (
            study_df.select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id, ColNames.date, ColNames.dps)
            .union(before_reg.withColumn(ColNames.dps, F.lit(0)))
            .union(after_reg.withColumn(ColNames.dps, F.lit(0)))
            .groupby(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)
            .agg(
                F.sum(ColNames.dps).cast(IntegerType()).alias(ColNames.mps),
                F.array_sort(F.collect_set(ColNames.date)).alias("dates"),
            )
            .filter(F.col(ColNames.mps) > 0)
            .withColumn(
                "more_metrics",
                frequency_and_regularity(
                    F.col("dates"),
                    F.lit(self.current_mt_period["month_start"]),
                    F.lit(self.current_mt_period["extended_month_start"]),
                    F.lit(self.current_mt_period["month_end"]),
                    F.lit(self.current_mt_period["extended_month_end"]),
                ),
            )
            .drop("dates")
            .withColumn(ColNames.frequency, F.col("more_metrics")[0].cast(IntegerType()))
            .withColumn(
                ColNames.regularity_mean,
                F.col("more_metrics")[1],
            )
            .withColumn(ColNames.regularity_std, F.col("more_metrics")[2])
            .withColumn(
                ColNames.id_type,
                F.when(F.col(ColNames.grid_id) == F.lit("unknown"), F.lit("unknown")).otherwise(F.lit("grid")),
            )
            .drop("more_metrics")
            .union(observation_df)
        )

        return combined_df

    def transform(self):
        # Load all needed dps
        if self.time_interval == "all":
            time_interval_start = dt.time(hour=self.day_start_hour)
            time_interval_end = dt.time(hour=self.day_start_hour)
        else:
            time_interval_start = getattr(self, f"{self.time_interval}_start")
            time_interval_end = getattr(self, f"{self.time_interval}_end")

        # Keep only time slots belonging to the time interval
        filtered = self.filter_dps_by_time_interval(
            self.current_dps_data, self.time_interval, time_interval_start, time_interval_end
        )

        # Keep only time slots belonging to the day type
        filtered = self.filter_dps_by_day_type(filtered, self.day_type)

        # Compute DPS
        dps = self.compute_daily_permanence_score(filtered)

        # Compute metrics
        mps = self.compute_midterm_metrics(dps)

        mps = (
            mps.withColumn(ColNames.day_type, F.lit(self.day_type))
            .withColumn(ColNames.time_interval, F.lit(self.time_interval))
            .withColumn(ColNames.year, F.lit(self.current_mt_period["month_start"].year).cast(ShortType()))
            .withColumn(ColNames.month, F.lit(self.current_mt_period["month_start"].month).cast(ByteType()))
        )

        mps = mps.repartition(*SilverMidtermPermanenceScoreDataObject.PARTITION_COLUMNS)

        self.output_data_objects[SilverMidtermPermanenceScoreDataObject.ID].df = mps

    @get_execution_stats
    def execute(self):
        self.logger.info("Reading data objects...")
        self.read()
        self.logger.info("... data objects read!")

        midterm_daily_data = []

        self.logger.info("Validating DPS data for each mid-term period...")
        for i, mt_period in enumerate(self.midterm_periods):
            self.logger.info(
                f"... validating {mt_period['extended_month_start']} to {mt_period['extended_month_end']} ..."
            )
            midterm_daily_data.append(self._validate_and_load_daily_permanence_score(mt_period))
        self.logger.info("... all mid-term periods validated!")

        self.logger.info("Starting mid-term permanece score & metrics computation...")
        for i, mt_period in enumerate(self.midterm_periods):
            self.current_mt_period = mt_period
            self.current_dps_data = midterm_daily_data[i]
            self.logger.info(f"... working on month {mt_period['month_start']} to {mt_period['month_end']}")

            self.footprint = self.input_data_objects[SilverCellFootprintDataObject.ID].df
            self.footprint = self.footprint.select(
                ColNames.cell_id, ColNames.grid_id, ColNames.year, ColNames.month, ColNames.day
            ).filter(
                F.make_date(ColNames.year, ColNames.month, ColNames.day).between(
                    self.current_mt_period["extended_month_start"] - dt.timedelta(days=1),
                    self.current_mt_period["extended_month_end"] + dt.timedelta(days=1),
                )
            )

            for day_type, time_intervals in self.period_combinations.items():
                for time_interval in time_intervals:
                    self.day_type = day_type
                    self.time_interval = time_interval
                    self.transform()
                    self.write()
                    self.logger.info(
                        f"... finished saving results for day_type `{self.day_type}` and time_interval `{self.time_interval}`"
                    )
            self.logger.info(
                f"... finished saving results for month {mt_period['month_start']} to {mt_period['month_end']}"
            )

        self.logger.info("... Finished!")
