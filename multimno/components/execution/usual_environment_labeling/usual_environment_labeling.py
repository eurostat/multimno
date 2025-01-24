"""
Module that implements the Usual Environment Labeling functionality
"""

import datetime as dt
import calendar as cal
from functools import reduce
from typing import Union, List, Dict, Tuple

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from multimno.core.component import Component

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.period_names import SEASONS
from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_usual_environment_labeling_quality_metrics_data_object import (
    SilverUsualEnvironmentLabelingQualityMetricsDataObject,
)
from multimno.core.log import get_execution_stats
from multimno.core.grid import InspireGridGenerator


class UsualEnvironmentLabeling(Component):
    """
    A class to calculate the grid tiles that conform the usual environment, home and work locations of each user.
    """

    COMPONENT_ID = "UsualEnvironmentLabeling"
    LABEL_TO_LABELNAMES = {"ue": "no_label", "home": "home", "work": "work"}
    LABEL_TO_SHORT_LABELNAMES = {"ue": "ue", "home": "h", "work": "w"}

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.gap_ps_thresholds = {
            "ue": self.config.getfloat(self.COMPONENT_ID, "ue_gap_ps_threshold"),
            "home": self.config.getint(self.COMPONENT_ID, "gap_ps_threshold"),
            "work": self.config.getint(self.COMPONENT_ID, "gap_ps_threshold"),
        }

        self.gap_ps_threshold_is_absolute = {
            "ue": False,
            "home": True,
            "work": True,
        }  # TODO:

        self.ps_thresholds = {
            "ue": self.config.getfloat(self.COMPONENT_ID, "ue_ps_threshold"),
            "home": self.config.getfloat(self.COMPONENT_ID, "home_ps_threshold"),
            "work": self.config.getfloat(self.COMPONENT_ID, "work_ps_threshold"),
        }

        self.freq_thresholds = {
            "ue": self.config.getfloat(self.COMPONENT_ID, "ue_ndays_threshold"),
            "home": self.config.getfloat(self.COMPONENT_ID, "home_ndays_threshold"),
            "work": self.config.getfloat(self.COMPONENT_ID, "work_ndays_threshold"),
        }

        self.ps_threshold_for_rare_devices = self.config.getfloat(self.COMPONENT_ID, "total_ps_threshold")
        self.freq_threshold_for_discontinuous_devices = self.config.getfloat(self.COMPONENT_ID, "total_ndays_threshold")

        self.day_and_interval_type_combinations = {
            "ue": [("all", "all"), ("all", "night_time"), ("workdays", "working_hours")],
            "home": [("all", "all"), ("all", "night_time")],
            "work": [("workdays", "working_hours")],
        }

        self.season = self.config.get(self.COMPONENT_ID, "season")
        if self.season not in SEASONS:
            error_msg = f"season: expected one of: {', '.join(SEASONS)} - found: {self.season}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

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
            raise ValueError(f"End month `{end_month}` should not be earlier than start month `{start_month}`")

        self.ltps_df: DataFrame = None
        self.rare_devices_count = 0
        self.discontinuous_devices_count = 0
        self.disaggregate_to_100m_grid = self.config.getboolean(
            self.COMPONENT_ID, "disaggregate_to_100m_grid", fallback=False
        )
        self.grid_gen = InspireGridGenerator(self.spark)

    def initalize_data_objects(self):
        # Load paths from configuration file:
        input_ltps_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "longterm_permanence_score_data_silver")
        output_uelabels_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "usual_environment_labels_data_silver")
        output_quality_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "usual_environment_labeling_quality_metrics_data_silver"
        )

        # Initialise input and output data objects:
        silver_ltps = SilverLongtermPermanenceScoreDataObject(self.spark, input_ltps_silver_path)
        ue_labels = SilverUsualEnvironmentLabelsDataObject(self.spark, output_uelabels_path)
        ue_quality_metrics = SilverUsualEnvironmentLabelingQualityMetricsDataObject(
            self.spark, output_quality_metrics_path
        )

        # Store data objects in the corresponding attributes:
        self.input_data_objects = {silver_ltps.ID: silver_ltps}
        self.output_data_objects = {ue_labels.ID: ue_labels, ue_quality_metrics.ID: ue_quality_metrics}

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        # read input data object:
        self.read()
        full_ltps_df = self.input_data_objects[SilverLongtermPermanenceScoreDataObject.ID].df
        # filtering to obtain the main dataset which this method will work with:
        self.ltps_df = self.filter_ltps_by_target_dates(full_ltps_df, self.start_date, self.end_date, self.season)
        # assert that all the needed day type and interval times are available in the main dataset:
        self.check_needed_day_and_interval_types(self.ltps_df, self.day_and_interval_type_combinations)
        partition_chunks = self._get_partition_chunks()
        for i, partition_chunk in enumerate(partition_chunks):
            self.logger.info(f"Processing partition chunk: {i}")
            self.logger.debug(f"Partition chunk: {partition_chunk}")
            self.partition_chunk = partition_chunk
            # main transformations of this method:
            self.transform()
            # write output data objects:
            self.write()
            self.logger.info(f"Finished {self.COMPONENT_ID}")

    @staticmethod
    def filter_ltps_by_target_dates(
        full_ltps_df: DataFrame, start_date: dt.date, end_date: dt.date, season: str
    ) -> DataFrame:
        """
        Keep only the rows of the input SilverLongtermPermanenceScoreDataObject in which the start
        date, end date and season match the ones specified for the processing of this method.

        Args:
            full_ltps_df (DataFrame): full dataset.
            start_date (dt.date): specified target start date for the execution of the method.
            end_date (dt.date): specified target end date for the execution of the method.
            season (str): specified target season for the execution of the method.

        Returns:
            DataFrame: filtered dataset.
        """
        filtered_ltps_df = full_ltps_df.filter(
            (F.col(ColNames.start_date) == start_date)
            & (F.col(ColNames.end_date) == end_date)
            & (F.col(ColNames.season) == season)
        ).select(
            ColNames.user_id,
            ColNames.grid_id,
            ColNames.lps,
            ColNames.total_frequency,
            ColNames.day_type,
            ColNames.time_interval,
            ColNames.user_id_modulo,
            ColNames.id_type,
        )
        return filtered_ltps_df

    @staticmethod
    def check_needed_day_and_interval_types(
        ltps_df: DataFrame, day_and_interval_type_combinations: Dict[str, List[Tuple[str, str]]]
    ):
        """
        Method that checks if the needed combinations of day type and interval type are available
        in the provided dataset.

        Args:
            ltps_df (DataFrame): provided Long-Term Permanence Score dataset
            day_and_interval_type_combinations (Dict[str,List[Tuple[str,str]]]): day type and interval type
                combinations that are needed for the execution of the method.

        Raises:
            FileNotFoundError: If there is no data for one or more of the needed combinations of day type
                and interval type.
        """
        # Build set of all needed day-interval type combinations:
        all_day_and_interval_type_combinations = {
            comb for el in day_and_interval_type_combinations.values() for comb in el
        }

        # Assert that these combinations appear at least once in the input Long-Term Permanence
        # Score data object:
        for day_type, time_interval in all_day_and_interval_type_combinations:

            filtered_ltps_df = ltps_df.filter(
                (F.col(ColNames.day_type) == day_type) & (F.col(ColNames.time_interval) == time_interval)
            )

            data_exists = filtered_ltps_df.count() > 0
            if not data_exists:
                raise FileNotFoundError(
                    "No Long-term Permanence Score data has been found for "
                    f"day_type `{day_type}` and time_interval `{time_interval}`"
                )

    @staticmethod
    def find_rarely_observed_devices(total_observations_df: DataFrame, total_device_ps_threshold: int) -> DataFrame:
        """
        Find devices (user ids) which match the condition for being considered "rarely observed".
        This condition consists in having a total device observation of: lps > total_ps_threshold.

        Args:
            total_observations_df (DataFrame): Long-Term Permanence Score dataset, filtered by the
                corresponding start and end dates, and filtered by id_type == 'device_observation'.
            total_device_ps_threshold (int): ps threshold.

        Returns:
            DataFrame: two-column dataframe with the ids of the devices to discard and the corresponding user modulos.
        """
        rarely_observed_user_ids = total_observations_df.filter(F.col(ColNames.lps) < total_device_ps_threshold).select(
            ColNames.user_id_modulo, ColNames.user_id
        )
        return rarely_observed_user_ids

    @staticmethod
    def find_discontinuously_observed_devices(
        total_observations_df: DataFrame, total_device_freq_threshold: int
    ) -> DataFrame:
        """
        Find devices (user ids) which match the condition for being considered "discontinuously observed".
        This condition consists in having a total device observation of: freq > total_freq_threshold.

        Args:
            total_observations_df (DataFrame): Long-Term Permanence Score dataset, filtered by the
                corresponding start and end dates, and filtered by id_type == 'device_observation'.
            total_device_freq_threshold (int): ps threshold.

        Returns:
            DataFrame: two-column dataframe with the ids of the devices to discard and the corresponding user modulos.
        """
        discontinuously_observed_user_ids = total_observations_df.filter(
            F.col(ColNames.total_frequency) < total_device_freq_threshold
        ).select(ColNames.user_id_modulo, ColNames.user_id)
        return discontinuously_observed_user_ids

    @staticmethod
    def discard_user_ids(ltps_df: DataFrame, user_ids: DataFrame) -> DataFrame:
        """
        Given a list of user ids, discard them from a Long-Term Permanence Score dataset.

        Args:
            ltps_df (DataFrame): Long-Term Permanence Score dataset.
            user_ids (DataFrame): one-column dataframe with the ids of the devices to discard.

        Returns:
            DataFrame: filtered Long-Term Permanence Score dataset.
        """
        ltps_df = ltps_df.join(user_ids, on=[ColNames.user_id_modulo, ColNames.user_id], how="left_anti")
        return ltps_df

    def discard_devices(self, ltps_df: DataFrame) -> DataFrame:
        """
        Given a Long-Term Permanence Score dataset (filtered for the corresponding start and end dates),
        discard the rows corresponding to some devices.

        There are 2 type of devices to discard:
            - rarely observed devices, based on LPS (long-term permanence score).
            - discontinuously observed devices, based on frequency.

        The user ids that are classified in any of these 2 groups are discarded from the Long-Term
        Permanence Score dataset, and the number of discarded users of each kind is saved to the
        corresponding attributes.

        Args:
            ltps_df (DataFrame): Long-Term Permanence Score dataset (filtered for the corresponding
                start and end dates).

        Returns:
            DataFrame: Long-Term Permanence Score dataset (filtered for the corresponding start and
                end dates), without the rows associated to rarely or discontinuously observed
                devices.
        """
        # Initial filter of ltps dataset to keep total device observation values:
        total_observations_df = ltps_df.filter(
            (F.col(ColNames.id_type) == "device_observation")
            & (F.col(ColNames.day_type) == "all")
            & (F.col(ColNames.time_interval) == "all")
        )

        # Rarely observed:
        rarely_observed_user_ids = self.find_rarely_observed_devices(
            total_observations_df, self.ps_threshold_for_rare_devices
        )
        self.rare_devices_count = rarely_observed_user_ids.count()

        # Discontinuously observed:
        discontinuously_observed_user_ids = self.find_discontinuously_observed_devices(
            total_observations_df, self.freq_threshold_for_discontinuous_devices
        )
        self.discontinuous_devices_count = discontinuously_observed_user_ids.count()

        # All user ids to discard:
        discardable_user_ids = rarely_observed_user_ids.union(discontinuously_observed_user_ids)

        # Filter dataset:
        filtered_ltps_df = self.discard_user_ids(ltps_df, discardable_user_ids)

        return filtered_ltps_df

    @staticmethod
    def add_abs_ps_threshold(
        ltps_df: DataFrame, window: Window, gap_ps_threshold: Union[int, float], threshold_is_absolute: bool
    ) -> DataFrame:
        """
        Add "abs_ps_threshold" field to the ltps dataset.

        If "threshold_is_absolute" is True, then just assign "abs_ps_threshold" = "gap_ps_threshold"
        value to all registers.

        If threshold_is_absolute is False, then reach the maximum value of lps for a grid tile for
        each device (ps_max) and calculate "abs_ps_threshold" = "gap_ps_threshold" * "ps_max".

        Args:
            ltps_df (DataFrame): Long-Term Permanence Score dataset.
            window (Window): window, partitioned by user id and ordered by lps.
            gap_ps_threshold (int|float): absolute/relative lps threshold.
            threshold_is_absolute (bool): indicates if gap_ps_threshold is a relative/absolute value.

        Returns:
            DataFrame: Long-Term Permanence Score dataset with the additional "abs_ps_threshold"
                column.
        """
        if threshold_is_absolute is True:
            ltps_df = ltps_df.withColumn("abs_ps_threshold", F.lit(gap_ps_threshold))
        else:
            ltps_df = (
                ltps_df.withColumn("ps_max", F.first(ColNames.lps).over(window))
                .withColumn("abs_ps_threshold", F.col("ps_max") * F.lit(gap_ps_threshold / 100))
                .drop("ps_max")
            )
        return ltps_df

    def cut_tiles_at_gap(self, ltps_df: DataFrame, gap_ps_threshold: float, threshold_is_absolute: bool) -> DataFrame:
        """
        Preprocessing function. Given a Long-Term Permanence Metrics dataset, for each user:
        - sort the grid tiles of the user by LPS
        - find a high difference (gap) in the LPS values between one grid tile and the next one,
          where what is a high difference is defined through the gap_ps_threshold argument.
        - for each agent, filter out all tiles after this high difference: the remaining tiles are
          the "pre-selected tiles", which are the output of this function.

        Args:
            ltps_df (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and
                time interval combination.
            gap_ps_threshold (int/float): absolute/relative lps threshold.
            threshold_is_absolute (bool): indicates if gap_ps_threshold is a relative/absolute value.

        Returns:
            DataFrame: dataset with the pre-selected tiles.
        """
        ltps_df = ltps_df.filter(F.col(ColNames.id_type) == "grid")

        window = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id).orderBy(F.desc(ColNames.lps))
        cumulative_window = window.rowsBetween(Window.unboundedPreceding, Window.currentRow)

        ltps_df = self.add_abs_ps_threshold(ltps_df, window, gap_ps_threshold, threshold_is_absolute)

        pre_selected_tiles_df = (
            ltps_df.withColumn("previous_lps", F.lag(ColNames.lps, 1).over(window))
            .withColumn("lps_difference", F.col("previous_lps") - F.col(ColNames.lps))
            .fillna({"lps_difference": 0})
            .withColumn("high_difference", F.when(F.col("lps_difference") >= F.col("abs_ps_threshold"), 1).otherwise(0))
            .withColumn("cumulative_condition", F.sum(F.col("high_difference")).over(cumulative_window))
            .filter(F.col("cumulative_condition") == F.lit(0))
            .drop("lps_difference", "previous_lps", "high_difference", "cumulative_condition", "abs_ps_threshold")
        )

        return pre_selected_tiles_df

    @staticmethod
    def calculate_device_abs_ps_threshold(
        ltps_df_i: DataFrame, target_rows_ltps_df: DataFrame, perc_ps_threshold: float
    ) -> DataFrame:
        """
        Calculate the total assigned long-term permanence score for each device (total_device_ps) for a given day type
        and time interval and add this information to an additional column of the provided dataset. Then, based on
        this column, generate the absolute ps threshold to consider for each device by applying the corresponding
        configured percentage (perc_ps_threshold).

        Args:
            ltps_df_i (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time interval
                combination.
            target_rows_ltps_df (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time
                interval combination, with one 'id_type' == 'grid'.
            perc_ps_threshold (float): specified ps threshold (in percentage).

        Returns:
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                with one 'id_type' == 'grid', with an additional column named "abs_ps_threshold".
        """
        device_total_ps_df = (
            ltps_df_i.filter(F.col(ColNames.id_type) == "device_observation")
            .withColumnRenamed(ColNames.lps, "total_device_ps")
            .select(ColNames.user_id_modulo, ColNames.user_id, "total_device_ps")
        )

        target_rows_ltps_df = (
            target_rows_ltps_df.join(device_total_ps_df, on=[ColNames.user_id_modulo, ColNames.user_id])
            .withColumn("abs_ps_threshold", F.col("total_device_ps") * F.lit(perc_ps_threshold / 100))
            .drop("total_device_ps")
        )

        return target_rows_ltps_df

    @staticmethod
    def filter_by_ps(ltps_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply the long-term permanence score filter (ps) to a Long-Term Permanence Metrics dataset. Filter rows for
        which 'lps' > abs_ps_threshold.

        Args:
            ltps_df (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time interval
                combination.

        Returns:
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                keeping only the rows that DO pass the ps filter.
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                keeping only the rows that DO NOT pass the ps filter.
        """
        common_df = ltps_df.withColumn(
            "selected_flag", F.when(F.col(ColNames.lps) >= F.col("abs_ps_threshold"), True).otherwise(False)
        )

        selected_tiles_df = common_df.filter(F.col("selected_flag")).drop("abs_ps_threshold", "selected_flag")
        not_selected_tiles_df = common_df.filter(~F.col("selected_flag")).drop("abs_ps_threshold", "selected_flag")

        return selected_tiles_df, not_selected_tiles_df

    @staticmethod
    def calculate_device_abs_ndays_threshold(
        ltps_df_i: DataFrame, target_rows_ltps_df: DataFrame, perc_ndays_threshold: float
    ) -> DataFrame:
        """
        Calculate the total frequency (observed number of days) for each device (total_device_ndays) for a given day
        type and time interval and add this information to an additional column of the provided dataset. Then, based on
        this column, generate the absolute ndays threshold to consider for each device by applying the corresponding
        configured percentage (perc_ndays_threshold).

        Args:
            ltps_df_i (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time interval
                combination.
            target_rows_ltps_df (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time
                interval combination, with one 'id_type' == 'grid'.
            perc_ndays_threshold (float): specified ndays threshold (in percentage).

        Returns:
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                with one 'id_type' == 'grid', with an additional column named "abs_ndays_threshold".
        """
        device_total_ndays_df = (
            ltps_df_i.filter(F.col(ColNames.id_type) == "device_observation")
            .withColumnRenamed(ColNames.total_frequency, "total_device_ndays")
            .select(ColNames.user_id_modulo, ColNames.user_id, "total_device_ndays")
        )

        target_rows_ltps_df = (
            target_rows_ltps_df.join(device_total_ndays_df, on=[ColNames.user_id_modulo, ColNames.user_id])
            .withColumn("abs_ndays_threshold", F.col("total_device_ndays") * F.lit(perc_ndays_threshold / 100))
            .drop("total_device_ndays")
        )

        return target_rows_ltps_df

    @staticmethod
    def filter_by_ndays(ltps_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply the frequency filter (number of days) to a Long-Term Permanence Metrics dataset. Filter rows
        for which 'total_frequency' > abs_ndays_threshold.

        Args:
            ltps_df (DataFrame): Long-Term Permanence Metrics dataset, for a specific day type and time interval
                combination.

        Returns:
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                keeping only the rows that DO pass the ndays filter.
            DataFrame: Long-Term Permanence Metrics dataset, for a specific day type and time interval combination,
                keeping only the rows that DO NOT pass the ndays filter.
        """
        common_df = ltps_df.withColumn(
            "selected_flag",
            F.when(F.col(ColNames.total_frequency) >= F.col("abs_ndays_threshold"), True).otherwise(False),
        )

        selected_tiles_df = common_df.filter(F.col("selected_flag")).drop("abs_ndays_threshold", "selected_flag")
        not_selected_tiles_df = common_df.filter(~F.col("selected_flag")).drop("abs_ndays_threshold", "selected_flag")

        return selected_tiles_df, not_selected_tiles_df

    def format_selected_tiles(self, selected_tiles_df: DataFrame, label_type: str, n_rule: int) -> DataFrame:
        """
        Add the "label", "ue_label_rule" and "location_label_rule" columns to the given selected tiles dataframe:
        - the "label" column is assigned directly depending on the label type being computed by mapping with the
          provided dictionary: LABEL_TO_LABELNAMES[label_type].
        - the "ue_label_rule" is equal to "ue_na" if the label type being computed is different from "ue", or is
          equal to "{ue}_{N}" if the label type being computed is "ue", where:
            - {N} = n_rule
            - {ue} = LABEL_TO_SHORT_LABELNAMES[label_type].
        - the "location_label_rule" is equal to "loc_na" if the label type being computed is "ue", or is equal to
          "{loc}_{N}" if the label type being computed is NOT "ue", where:
            - {N} = n_rule
            - {loc} = LABEL_TO_SHORT_LABELNAMES[label_type].

        Args:
            selected_tiles_df (DataFrame): selected tiles dataset.
            label_type (str): label type to compute: 'ue', 'home' or 'work'.
            n_rule (int): number of the current rule in order to generate the label_rule column values.

        Returns:
            DataFrame: selected tiles dataset, with the additional columns: "label", "ue_label_rule" and
                "location_label_rule".
        """
        short_labelname = self.LABEL_TO_SHORT_LABELNAMES[label_type]

        if label_type == "ue":
            ue_rule_txt = f"{short_labelname}_{n_rule}"
            selected_tiles_df = selected_tiles_df.withColumn(ColNames.ue_label_rule, F.lit(ue_rule_txt))
        else:
            labelname = self.LABEL_TO_LABELNAMES[label_type]
            selected_tiles_df = selected_tiles_df.withColumn(ColNames.label, F.lit(labelname))
            loc_rule_txt = f"{short_labelname}_{n_rule}"
            selected_tiles_df = selected_tiles_df.withColumn(ColNames.location_label_rule, F.lit(loc_rule_txt))

        return selected_tiles_df

    def format_not_selected_tiles(self, not_selected_tiles_df: DataFrame, label_type: str) -> DataFrame:
        """
        Add the "label", "ue_label_rule" and "location_label_rule" columns to the not selected tiles dataframe:
        - the "label" column is assigned directly depending on the label type being computed by mapping with the
          provided dictionary: LABEL_TO_LABELNAMES[label_type].
        - the "ue_label_rule" is equal to "ue_na".
        - the "location_label_rule" is equal to "loc_na".

        Args:
            not_selected_tiles_df (DataFrame): not selected tiles dataset.
            label_type (str): label type to compute: 'ue', 'home' or 'work'.

        Returns:
            DataFrame: not selected tiles dataset, with the additional columns: "label", "ue_label_rule" and
                "location_label_rule".
        """
        if label_type == "ue":
            not_selected_tiles_df = not_selected_tiles_df.withColumn(ColNames.ue_label_rule, F.lit("ue_na"))
        else:
            labelname = self.LABEL_TO_LABELNAMES[label_type]
            not_selected_tiles_df = not_selected_tiles_df.withColumn(ColNames.label, F.lit(labelname))
            not_selected_tiles_df = not_selected_tiles_df.withColumn(ColNames.location_label_rule, F.lit("loc_na"))

        return not_selected_tiles_df

    def compute_generic_labeling(self, label_type: str, apply_ndays_filter: bool) -> DataFrame:
        """
        Generate the labeled tiles dataset for the specified label type. This function is generic and works for all
        label types.

        Args:
            label_type (str): label type to compute: 'ue', 'home' or 'work'.
            apply_ndays_filter (bool): Indicates if the final ndays frequency filter shall be applied in the computation
                of the specified label type.

        Returns:
            DataFrame: labeled tiles dataset for the specified label type.
        """
        ps_threshold = self.ps_thresholds[label_type]
        gap_ps_threshold = self.gap_ps_thresholds[label_type]
        gap_ps_threshold_is_absolute = self.gap_ps_threshold_is_absolute[label_type]
        ndays_threshold = self.freq_thresholds[label_type]
        day_and_interval_combinations = self.day_and_interval_type_combinations[label_type]

        selected_tiles_dfs_list = []

        ##### 1st STAGE #####
        for i, (day_type, time_interval) in enumerate(day_and_interval_combinations):

            # filter ltps df for the current day type and time interval combination:
            ltps_df_i = self.ltps_df.filter(
                (F.col(ColNames.day_type) == day_type) & (F.col(ColNames.time_interval) == time_interval)
            )

            # if first day_type, time_interval combination:
            if i == 0:
                # cut tiles at gap to generate preselected tiles
                tiles_before_gap_df = self.cut_tiles_at_gap(ltps_df_i, gap_ps_threshold, gap_ps_threshold_is_absolute)
                preselected_tiles_df = tiles_before_gap_df

            # rest of day_type, time_interval combinations:
            else:
                # reach not selected tiles from previous iteration to generate preselected tiles
                preselected_tiles_df = ltps_df_i.join(
                    not_selected_tiles_df.select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id),
                    on=[ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id],
                )

            # add abs_ps_threshold column to dataframe:
            preselected_tiles_df = self.calculate_device_abs_ps_threshold(ltps_df_i, preselected_tiles_df, ps_threshold)

            # apply relative lps filter to obtain selected tiles and not selected tiles:
            selected_tiles_df, not_selected_tiles_df = self.filter_by_ps(preselected_tiles_df)

            # format current selected tiles df and add to list:
            n_rule = 1 if i == 0 else 2
            selected_tiles_df = self.format_selected_tiles(selected_tiles_df, label_type, n_rule)
            selected_tiles_dfs_list.append(selected_tiles_df)

        ##### 2nd STAGE #####
        if apply_ndays_filter is True:

            # filter ltps df for the first day type and time interval combination:
            first_day_type, first_time_interval = day_and_interval_combinations[0]
            ltps_df_i = self.ltps_df.filter(
                (F.col(ColNames.day_type) == first_day_type) & (F.col(ColNames.time_interval) == first_time_interval)
            )

            # add abs_ndays_threshold column to dataframe:
            preselected_tiles_df = self.calculate_device_abs_ndays_threshold(
                ltps_df_i, not_selected_tiles_df, ndays_threshold
            )

            # apply relative lps filter to obtain selected tiles and not selected tiles:
            selected_tiles_df, not_selected_tiles_df = self.filter_by_ndays(preselected_tiles_df)

            # format current selected tiles df and add to list:
            n_rule = 2 if len(day_and_interval_combinations) == 1 else 3
            selected_tiles_df = self.format_selected_tiles(selected_tiles_df, label_type, n_rule)
            selected_tiles_dfs_list.append(selected_tiles_df)

        ##### Concatenate to generate final dataset for this label type #####
        # format not selected tiles df and add to list
        not_selected_tiles_df = self.format_not_selected_tiles(not_selected_tiles_df, label_type)
        selected_tiles_dfs_list.append(not_selected_tiles_df)

        # concatenate all selected and not selected
        labeled_tiles_df = reduce(DataFrame.unionAll, selected_tiles_dfs_list)

        # select columns for output
        base_columns = [
            ColNames.user_id,
            ColNames.grid_id,
            ColNames.user_id_modulo,
        ]
        if label_type == "ue":
            columns = base_columns + [ColNames.ue_label_rule]
        else:
            columns = base_columns + [ColNames.label, ColNames.location_label_rule]

        labeled_tiles_df = labeled_tiles_df.select(columns)

        return labeled_tiles_df

    def compute_ue_labeling(self) -> DataFrame:
        """
        Generate dataset with the tiles that conform the UE (Usual Environment) of each device.

        Returns:
            DataFrame: UE tiles dataset.
        """
        ue_tiles_df = self.compute_generic_labeling(label_type="ue", apply_ndays_filter=False)
        return ue_tiles_df

    def compute_home_labeling(self) -> DataFrame:
        """
        Generate dataset with the tiles that conform the home location of each device.

        Returns:
            DataFrame: home tiles dataset.
        """
        home_tiles_df = self.compute_generic_labeling(label_type="home", apply_ndays_filter=True)
        return home_tiles_df

    def compute_work_labeling(self) -> DataFrame:
        """
        Generate dataset with the tiles that conform the work location of each device.

        Returns:
            DataFrame: work tiles dataset.
        """
        work_tiles_df = self.compute_generic_labeling(label_type="work", apply_ndays_filter=True)
        return work_tiles_df

    def concatenate_labeled_tiles(
        self, ue_tiles_df: DataFrame, home_tiles_df: DataFrame, work_tiles_df: DataFrame
    ) -> DataFrame:
        """
        Concatenate UE tiles, home tiles and work tiles datasets into a unique dataset.

        Args:
            ue_tiles_df (DataFrame): UE tiles dataset.
            home_tiles_df (DataFrame): home tiles dataset.
            work_tiles_df (DataFrame): work tiles dataset.

        Returns:
            DataFrame: Usual Environment Labels dataframe.
        """
        # just concatenate home and work tiles:
        loc_tiles_df = home_tiles_df.union(work_tiles_df)

        labels_df = (
            loc_tiles_df.join(
                ue_tiles_df,
                on=[ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id],
                how="outer",
            )
            .select(
                F.col(ColNames.user_id_modulo),
                F.col(ColNames.user_id),
                F.col(ColNames.grid_id),
                F.col(ColNames.label),
                F.col(ColNames.ue_label_rule),
                F.col(ColNames.location_label_rule),
            )
            .fillna(
                {ColNames.label: "no_label", ColNames.ue_label_rule: "ue_na", ColNames.location_label_rule: "loc_na"}
            )
        )

        labels_df = (
            labels_df.withColumn(ColNames.season, F.lit(self.season))
            .withColumn(ColNames.start_date, F.lit(self.start_date))
            .withColumn(ColNames.end_date, F.lit(self.end_date))
        )

        return labels_df

    @staticmethod
    def get_rule_count(df: DataFrame, col_to_value: Dict[str, str]) -> int:
        """
        Sums the count column values of the given dataframe for the corresponding filter (col_to_value).

        Args:
            df (DataFrame): input dataframe.
            col_to_value (Dict[str, str]): filter to apply.

        Returns:
            int: count column sum.
        """
        conditions = (F.col(colname) == colvalue for colname, colvalue in col_to_value.items())
        combined_condition = reduce(lambda x, y: x & y, conditions)
        count_value = df.filter(combined_condition).agg(F.sum("count")).collect()[0][0]
        if count_value is None:
            count_value = 0
        return count_value

    def generate_quality_metrics(self, ue_labels_df: DataFrame) -> DataFrame:
        """
        Build usual environment labeling quality metrics dataframe.

        Args:
            ue_labels_df (DataFrame): usual environment labels dataframe.

        Returns:
            DataFrame: quality metrics dataframe.
        """
        grouped_df = ue_labels_df.groupby(ColNames.label, ColNames.ue_label_rule, ColNames.location_label_rule).count()
        grouped_df.cache()

        ue_1_rule_count = self.get_rule_count(grouped_df, {ColNames.ue_label_rule: "ue_1"})
        ue_2_rule_count = self.get_rule_count(grouped_df, {ColNames.ue_label_rule: "ue_2"})
        ue_3_rule_count = self.get_rule_count(grouped_df, {ColNames.ue_label_rule: "ue_3"})
        h_1_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "h_1"})
        h_2_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "h_2"})
        h_3_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "h_3"})
        w_1_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "w_1"})
        w_2_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "w_2"})
        w_3_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "w_3"})
        ue_na_rule_count = self.get_rule_count(grouped_df, {ColNames.ue_label_rule: "ue_na"})
        loc_na_rule_count = self.get_rule_count(grouped_df, {ColNames.location_label_rule: "loc_na"})
        h_non_ue_count = self.get_rule_count(grouped_df, {ColNames.label: "home", ColNames.ue_label_rule: "ue_na"})
        w_non_ue_count = self.get_rule_count(grouped_df, {ColNames.label: "work", ColNames.ue_label_rule: "ue_na"})

        grouped_df.unpersist()

        data = [
            ("device_filter_1_rule", self.rare_devices_count, self.start_date, self.end_date, self.season),
            ("device_filter_2_rule", self.discontinuous_devices_count, self.start_date, self.end_date, self.season),
            ("ue_1_rule", ue_1_rule_count, self.start_date, self.end_date, self.season),
            ("ue_2_rule", ue_2_rule_count, self.start_date, self.end_date, self.season),
            ("ue_3_rule", ue_3_rule_count, self.start_date, self.end_date, self.season),
            ("h_1_rule", h_1_rule_count, self.start_date, self.end_date, self.season),
            ("h_2_rule", h_2_rule_count, self.start_date, self.end_date, self.season),
            ("h_3_rule", h_3_rule_count, self.start_date, self.end_date, self.season),
            ("w_1_rule", w_1_rule_count, self.start_date, self.end_date, self.season),
            ("w_2_rule", w_2_rule_count, self.start_date, self.end_date, self.season),
            ("w_3_rule", w_3_rule_count, self.start_date, self.end_date, self.season),
            ("ue_na_rule", ue_na_rule_count, self.start_date, self.end_date, self.season),
            ("loc_na_rule", loc_na_rule_count, self.start_date, self.end_date, self.season),
            ("h_non_ue", h_non_ue_count, self.start_date, self.end_date, self.season),
            ("w_non_ue", w_non_ue_count, self.start_date, self.end_date, self.season),
        ]

        # Create DataFrame
        schema = SilverUsualEnvironmentLabelingQualityMetricsDataObject.SCHEMA
        quality_metrics_df = self.spark.createDataFrame(data, schema)

        return quality_metrics_df

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        if self.partition_chunk is not None:
            self.ltps_df = self.ltps_df.filter(F.col(ColNames.user_id_modulo).isin(self.partition_chunk))

        # discard devices that will not be analysed ('rarely observed' or 'discontinuously observed'):
        self.ltps_df = self.discard_devices(self.ltps_df)

        # calculate tiles that belong to the ue (usual environment) of each device:
        ue_tiles_df = self.compute_ue_labeling()

        # calculate tiles that belong to the home location of each device:
        home_tiles_df = self.compute_home_labeling()

        # calculate tiles that belong to the work location of each device:
        work_tiles_df = self.compute_work_labeling()

        # join ue tiles, home tiles and work tiles datasets into a ue labels dataset:
        labeled_tiles_df = self.concatenate_labeled_tiles(ue_tiles_df, home_tiles_df, work_tiles_df)

        # generate labeling quality metrics dataset:
        labeling_quality_metrics_df = self.generate_quality_metrics(labeled_tiles_df)

        # Repartition
        if self.disaggregate_to_100m_grid:
            self.logger.info("Dissagregating 200m to 100m grid")
            labeled_tiles_df = self.grid_gen.get_children_grid_ids(labeled_tiles_df, 200, 100)
        labeled_tiles_df = labeled_tiles_df.repartition(*SilverUsualEnvironmentLabelsDataObject.PARTITION_COLUMNS)
        labeling_quality_metrics_df = labeling_quality_metrics_df.repartition(
            *SilverUsualEnvironmentLabelingQualityMetricsDataObject.PARTITION_COLUMNS
        )

        # save objects to output data dict:
        self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].df = labeled_tiles_df
        self.output_data_objects[SilverUsualEnvironmentLabelingQualityMetricsDataObject.ID].df = (
            labeling_quality_metrics_df
        )

    def _get_partition_chunks(self) -> List[List[int]]:
        """
        Method that returns the partition chunks for the current date.

        Returns:
            List[List[int, int]]: list of partition chunks. If the partition_chunk_size is not defined in the config or
                the number of partitions is less than the desired chunk size, it will return a list with a single None element.
        """
        # Get partitions desired
        partition_chunk_size = self.config.getint(self.COMPONENT_ID, "partition_chunk_size", fallback=None)
        number_of_partitions = self.config.getint(self.COMPONENT_ID, "number_of_partitions", fallback=None)

        if partition_chunk_size is None or number_of_partitions is None or partition_chunk_size <= 0:
            return [None]

        if number_of_partitions <= partition_chunk_size:
            self.logger.warning(
                f"Available Partition number ({number_of_partitions}) is "
                f"less than the desired chunk size ({partition_chunk_size}). "
                f"Using all partitions."
            )
            return [None]
        partition_chunks = [
            list(range(i, min(i + partition_chunk_size, number_of_partitions)))
            for i in range(0, number_of_partitions, partition_chunk_size)
        ]
        # NOTE: Generate chunks if partition_values were read for each day
        # getting exactly the amount of partitions for that day

        # partition_chunks = [
        #     partition_values[i : i + partition_chunk_size]
        #     for i in range(0, partition_values_size, partition_chunk_size)
        # ]

        return partition_chunks
