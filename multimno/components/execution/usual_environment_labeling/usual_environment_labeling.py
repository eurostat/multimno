"""
Module that implements the Usual Environment Labeling functionality
"""

import datetime as dt
import calendar as cal
from functools import reduce
from typing import Union, List, Dict, Tuple, Set

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

from multimno.core.component import Component

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.period_names import Seasons, PeriodCombinations
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import UeGridIdType
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
from multimno.core.utils import apply_schema_casting
from multimno.core.grid import InspireGridGenerator
from multimno.core.spark_session import delete_file_or_folder


class UsualEnvironmentLabeling(Component):
    """
    A class to calculate the grid tiles that conform the usual environment, home and work locations of each user.
    """

    COMPONENT_ID = "UsualEnvironmentLabeling"
    LABEL_TO_LABELNAMES = {"ue": "no_label", "home": "home", "work": "work"}
    LABEL_TO_SHORT_LABELNAMES = {"ue": "ue", "home": "h", "work": "w"}

    TOTAL_PERMANENCE_THRESHOLD = "total_ps_threshold"
    TOTAL_FREQUENCY_THRESHOLD = "total_freq_threshold"

    CHECK_TO_COLUMN = {
        TOTAL_PERMANENCE_THRESHOLD: ColNames.lps,
        TOTAL_FREQUENCY_THRESHOLD: ColNames.total_frequency,
    }

    UNLABELED_TILES = "unlabeled_tiles"
    UNLABELED_DEVICES = "unlabeled_devices"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.gap_ps_thresholds = {
            "ue": self.config.getfloat(self.COMPONENT_ID, "ue_gap_ps_threshold"),
            "home": self.config.getint(self.COMPONENT_ID, "home_gap_ps_threshold"),
            "work": self.config.getint(self.COMPONENT_ID, "work_gap_ps_threshold"),
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
        if not Seasons.is_valid_type(self.season):
            error_msg = f"season: expected one of: {', '.join(Seasons.values)} - found: {self.season}"
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
        self.label_home = self.config.getboolean(self.COMPONENT_ID, "label_home", fallback=False)
        self.label_work = self.config.getboolean(self.COMPONENT_ID, "label_work", fallback=False)

    def initalize_data_objects(self):
        # Load paths from configuration file:
        input_ltps_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "longterm_permanence_score_data_silver")
        output_uelabels_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "usual_environment_labels_data_silver")
        self.ue_labels_cache_path = f"{output_uelabels_path}_cache"
        output_quality_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "usual_environment_labeling_quality_metrics_data_silver"
        )

        # Clear destination directory if needed
        clear_destination_directory = self.config.getboolean(
            self.COMPONENT_ID, "clear_destination_directory", fallback=False
        )
        if clear_destination_directory:
            self.logger.warning(f"Deleting: {output_uelabels_path}")
            delete_file_or_folder(self.spark, output_uelabels_path)

            self.logger.warning(f"Deleting: {output_quality_metrics_path}")
            delete_file_or_folder(self.spark, output_quality_metrics_path)

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

        # get label configuration:
        ue_labeling_config, meaningful_location_labeling_config = self.get_labeling_config()

        # check if meaningful location labeling is enabled and remove the corresponding label configurations:
        if not self.label_home:
            meaningful_location_labeling_config = [
                label for label in meaningful_location_labeling_config if label["label_code"] != "home"
            ]
        if not self.label_work:
            meaningful_location_labeling_config = [
                label for label in meaningful_location_labeling_config if label["label_code"] != "work"
            ]

        # extract relevant intervals:
        relevant_periods = self.extract_relevant_periods(ue_labeling_config)
        for label in meaningful_location_labeling_config:
            relevant_periods.update(self.extract_relevant_periods(label))
        # assert that all the needed day type and interval times are available in the main dataset:
        self.check_needed_day_and_interval_types(self.ltps_df, relevant_periods)
        self.ue_labeling_config = ue_labeling_config
        self.meaningful_location_labeling_config = meaningful_location_labeling_config

        partition_chunks = self._get_partition_chunks()
        for i, partition_chunk in enumerate(partition_chunks):
            self.logger.info(f"Processing partition chunk: {i}")
            self.logger.debug(f"Partition chunk: {partition_chunk}")
            self.partition_chunk = partition_chunk
            # main transformations of this method:
            self.transform()
            self.spark.catalog.clearCache()

        self.logger.info(f"Generating quality metrics...")
        quality_metrics_df = self.compute_quality_metrics()
        self.output_data_objects[SilverUsualEnvironmentLabelingQualityMetricsDataObject.ID].df = quality_metrics_df
        self.output_data_objects[SilverUsualEnvironmentLabelingQualityMetricsDataObject.ID].write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def get_labeling_config(self):
        """
        Get the configuration of the labeling process.

        Returns:
            Dict: configuration of the labeling process.
        """
        ue_labels = {
            "label_code": "ue",
            "ps_difference_gap_filter": {
                "relevant_periods": [PeriodCombinations.ALL_PERIODS],
                "gap_permanence_threshold": self.gap_ps_thresholds["ue"],
                "is_absolute": self.gap_ps_threshold_is_absolute["ue"],
            },
            "labeling_rules": [
                {
                    "rule_code": "ue_1",
                    "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                    "threshold_value": self.ps_thresholds["ue"],
                    "relevant_periods": [PeriodCombinations.ALL_PERIODS],
                    "apply_condition": self.UNLABELED_TILES,
                },
                {
                    "rule_code": "ue_2",
                    "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                    "threshold_value": self.ps_thresholds["ue"],
                    "relevant_periods": [
                        PeriodCombinations.NIGHT_TIME_ALL,
                    ],
                    "apply_condition": self.UNLABELED_TILES,
                },
                {
                    "rule_code": "ue_2",
                    "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                    "threshold_value": self.ps_thresholds["ue"],
                    "relevant_periods": [
                        PeriodCombinations.WORKING_HOURS_WORKDAYS,
                    ],
                    "apply_condition": self.UNLABELED_TILES,
                },
                {
                    "rule_code": "ue_3",
                    "check_type": self.TOTAL_FREQUENCY_THRESHOLD,
                    "threshold_value": self.freq_thresholds["ue"],
                    "relevant_periods": [PeriodCombinations.ALL_PERIODS],
                    "apply_condition": self.UNLABELED_TILES,
                },
            ],
        }

        meaningful_location_labels = [
            {
                "label_code": "home",
                "ps_difference_gap_filter": {
                    "relevant_periods": [PeriodCombinations.ALL_PERIODS],
                    "gap_permanence_threshold": self.gap_ps_thresholds["home"],
                    "is_absolute": self.gap_ps_threshold_is_absolute["home"],
                },
                "exclude_label_codes": [],
                "labeling_rules": [
                    {
                        "rule_code": "h_1",
                        "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                        "threshold_value": self.ps_thresholds["home"],
                        "relevant_periods": [PeriodCombinations.ALL_PERIODS],
                        "apply_condition": self.UNLABELED_DEVICES,
                    },
                    {
                        "rule_code": "h_2",
                        "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                        "threshold_value": self.ps_thresholds["home"],
                        "relevant_periods": [PeriodCombinations.NIGHT_TIME_ALL],
                        "apply_condition": self.UNLABELED_DEVICES,
                    },
                    {
                        "rule_code": "h_3",
                        "check_type": self.TOTAL_FREQUENCY_THRESHOLD,
                        "threshold_value": self.freq_thresholds["home"],
                        "relevant_periods": [PeriodCombinations.NIGHT_TIME_ALL],
                        "apply_condition": self.UNLABELED_DEVICES,
                    },
                ],
            },
            {
                "label_code": "work",
                "ps_difference_gap_filter": {
                    "relevant_periods": [PeriodCombinations.WORKING_HOURS_WORKDAYS],
                    "gap_permanence_threshold": self.gap_ps_thresholds["work"],
                    "is_absolute": self.gap_ps_threshold_is_absolute["work"],
                },
                "exclude_label_codes": [],
                "labeling_rules": [
                    {
                        "rule_code": "w_1",
                        "check_type": self.TOTAL_PERMANENCE_THRESHOLD,
                        "threshold_value": self.ps_thresholds["work"],
                        "relevant_periods": [PeriodCombinations.WORKING_HOURS_WORKDAYS],
                        "apply_condition": self.UNLABELED_DEVICES,
                    },
                    {
                        "rule_code": "w_2",
                        "check_type": self.TOTAL_FREQUENCY_THRESHOLD,
                        "threshold_value": self.freq_thresholds["work"],
                        "relevant_periods": [PeriodCombinations.WORKING_HOURS_WORKDAYS],
                        "apply_condition": self.UNLABELED_DEVICES,
                    },
                ],
            },
        ]

        return ue_labels, meaningful_location_labels

    def extract_relevant_periods(self, labeling_config: Dict) -> List[List[str]]:
        all_intervals = set()

        for rule in labeling_config["labeling_rules"]:
            for interval in rule["relevant_periods"]:
                all_intervals.add(tuple(interval))

        for interval in labeling_config["ps_difference_gap_filter"]["relevant_periods"]:
            all_intervals.add(tuple(interval))

        return all_intervals

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
            ColNames.season,
            ColNames.day_type,
            ColNames.time_interval,
            ColNames.user_id_modulo,
            ColNames.id_type,
        )
        return filtered_ltps_df

    @staticmethod
    def check_needed_day_and_interval_types(
        ltps_df: DataFrame, day_and_interval_type_combinations: Set[Tuple[str, str, str]]
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
        # Assert that these combinations appear at least once in the input Long-Term Permanence
        # Score data object:
        for season, day_type, time_interval in day_and_interval_type_combinations:
            filtered_ltps_df = ltps_df.filter(
                (F.col(ColNames.season) == season)
                & (F.col(ColNames.day_type) == day_type)
                & (F.col(ColNames.time_interval) == time_interval)
            )

            data_exists = filtered_ltps_df.count() > 0
            if not data_exists:
                raise FileNotFoundError(
                    "No Long-term Permanence Score data has been found for "
                    f"day_type `{day_type}` and time_interval `{time_interval}`"
                )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        if self.partition_chunk is not None:
            self.ltps_df = self.ltps_df.filter(F.col(ColNames.user_id_modulo).isin(self.partition_chunk))
        # discard devices that will not be analysed ('rarely observed' or 'discontinuously observed'):
        self.logger.info("Discarding devices...")
        self.ltps_df = self.discard_devices(self.ltps_df)

        # calculate tiles that belong to the ue (usual environment) of each device:
        self.logger.info("Calculating usual environment...")
        ue_tiles_df = self.compute_generic_labeling(self.ue_labeling_config, is_location_labeling=False)
        self.write_label(ue_tiles_df)

        for label_config in self.meaningful_location_labeling_config:
            self.logger.info(f"Calculating {label_config['label_code']} for ue tiles...")

            self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].read()
            ue_tiles_df = self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].df
            self.ue_tiles_df = ue_tiles_df.filter(
                (F.col(ColNames.season) == self.season)
                & (F.col(ColNames.start_date) == self.start_date)
                & (F.col(ColNames.end_date) == self.end_date)
                & (F.col(ColNames.label) == "ue")
            ).select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)

            labeled_tiles_df = self.compute_generic_labeling(label_config, is_location_labeling=True)
            self.write_label(labeled_tiles_df)

    def write_label(self, labeled_tiles_df: DataFrame):
        """
        Write the labeled tiles to the output data object.

        Args:
            labeled_tiles_df (DataFrame): labeled tiles dataset.
        """
        labeled_tiles_df = labeled_tiles_df.withColumn(ColNames.season, F.lit(self.season))
        labeled_tiles_df = labeled_tiles_df.withColumn(ColNames.start_date, F.lit(self.start_date))
        labeled_tiles_df = labeled_tiles_df.withColumn(ColNames.end_date, F.lit(self.end_date))

        # Repartition
        if self.disaggregate_to_100m_grid:
            self.logger.info("Dissagregating 200m to 100m grid")
            labeled_tiles_df = self.grid_gen.get_children_grid_ids(labeled_tiles_df, 200, 100)
        labeled_tiles_df = labeled_tiles_df.repartition(*SilverUsualEnvironmentLabelsDataObject.PARTITION_COLUMNS)

        labeled_tiles_df = apply_schema_casting(labeled_tiles_df, SilverUsualEnvironmentLabelsDataObject.SCHEMA)

        # save the output data object:
        self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].df = labeled_tiles_df
        self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].write()

        # clear cache
        self.spark.catalog.clearCache()

    @staticmethod
    def find_rarely_observed_devices(
        total_observations_df: DataFrame, total_device_threshold: int, threshold_col: str
    ) -> DataFrame:
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
        rarely_observed_user_ids = total_observations_df.filter(F.col(threshold_col) < total_device_threshold).select(
            ColNames.user_id_modulo, ColNames.user_id
        )
        return rarely_observed_user_ids

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
            (F.col(ColNames.id_type) == UeGridIdType.DEVICE_OBSERVATION_STR)
            & (F.col(ColNames.season) == PeriodCombinations.ALL_PERIODS[0])
            & (F.col(ColNames.day_type) == PeriodCombinations.ALL_PERIODS[1])
            & (F.col(ColNames.time_interval) == PeriodCombinations.ALL_PERIODS[2])
        )

        # Rarely observed:
        rarely_observed_user_ids = self.find_rarely_observed_devices(
            total_observations_df, self.ps_threshold_for_rare_devices, ColNames.lps
        )
        self.rare_devices_count = self.rare_devices_count + rarely_observed_user_ids.count()
        # Discontinuously observed:
        discontinuously_observed_user_ids = self.find_rarely_observed_devices(
            total_observations_df, self.freq_threshold_for_discontinuous_devices, ColNames.total_frequency
        )
        self.discontinuous_devices_count = self.discontinuous_devices_count + discontinuously_observed_user_ids.count()
        # All user ids to discard:
        discardable_user_ids = rarely_observed_user_ids.union(discontinuously_observed_user_ids).distinct()

        # Filter dataset:
        filtered_ltps_df = ltps_df.join(
            discardable_user_ids, on=[ColNames.user_id_modulo, ColNames.user_id], how="left_anti"
        )

        return filtered_ltps_df

    def compute_generic_labeling(
        self,
        labeling_config,
        is_location_labeling,
    ) -> DataFrame:
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
        label_code = labeling_config["label_code"]
        gap_ps_threshold = labeling_config["ps_difference_gap_filter"]["gap_permanence_threshold"]
        gap_ps_threshold_is_absolute = labeling_config["ps_difference_gap_filter"]["is_absolute"]
        gap_period_combinations = labeling_config["ps_difference_gap_filter"]["relevant_periods"][0]

        # filter ltps df for the current day type and time interval combination:
        ltps_df = self.ltps_df.filter(
            (F.col(ColNames.season) == gap_period_combinations[0])
            & (F.col(ColNames.day_type) == gap_period_combinations[1])
            & (F.col(ColNames.time_interval) == gap_period_combinations[2])
        )

        # if this is meaningful location labeling, use only tiles labeled as usual environment
        if is_location_labeling:
            ltps_df = ltps_df.join(self.ue_tiles_df, on=[ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id])

        # cut tiles at gap to generate preselected tiles
        preselected_tiles_df = self.cut_tiles_at_gap(ltps_df, gap_ps_threshold, gap_ps_threshold_is_absolute)

        labeled_tiles_dfs_list = []
        is_first_rule = True
        for rule in labeling_config["labeling_rules"]:
            self.logger.info(f"Applying rule: {rule['rule_code']}")
            rule_code = rule["rule_code"]
            rule_threshold = rule["threshold_value"]
            rule_check_type = rule["check_type"]
            rule_check_column = self.CHECK_TO_COLUMN[rule_check_type]
            rule_apply_condition = rule["apply_condition"]
            rule_period_combinations = rule["relevant_periods"][0]
            # use only tiles which are still unlabeled after the previous rule
            if not is_first_rule:
                preselected_tiles_df = labeled_tiles_df.filter(F.col("is_labeled") == False).select(
                    ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id
                )

            # filter ltps df for the current day type and time interval combination:
            ltps_df = self.ltps_df.filter(
                (F.col(ColNames.season) == rule_period_combinations[0])
                & (F.col(ColNames.day_type) == rule_period_combinations[1])
                & (F.col(ColNames.time_interval) == rule_period_combinations[2])
            )

            device_observation_df = ltps_df.filter(
                F.col(ColNames.id_type) == UeGridIdType.DEVICE_OBSERVATION_STR
            ).select(ColNames.user_id_modulo, ColNames.user_id, rule_check_column)

            # keep only the tiles that left after gap filtering and previous rules
            labeled_tiles_df = ltps_df.join(
                preselected_tiles_df.select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id),
                on=[ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id],
                how="right",
            )
            # might be the case that there are no inputs for current day type and time interval combination
            # for tiles that were left from previous rules. To keep them for the next rule, need to coallesce
            labeled_tiles_df = labeled_tiles_df.fillna({rule_check_column: 0})

            # apply rule to preselected tiles
            labeled_tiles_df = self.calculate_device_abs_threshold(
                device_observation_df, labeled_tiles_df, rule_threshold, rule_check_column
            )

            labeled_tiles_df = labeled_tiles_df.withColumn(
                "is_labeled", F.col(rule_check_column) >= F.col("abs_threshold")
            )

            labeled_tiles_df = labeled_tiles_df.withColumn(ColNames.label_rule, F.lit(rule_code))

            labeled_tiles_df = labeled_tiles_df.select(
                ColNames.user_id_modulo,
                ColNames.user_id,
                ColNames.grid_id,
                ColNames.id_type,
                F.col("is_labeled"),
                F.col(ColNames.label_rule),
            )

            labeled_tiles_df = labeled_tiles_df.persist()
            labeled_tiles_df.count()
            labeled_tiles_dfs_list.append(labeled_tiles_df.filter(F.col("is_labeled")))
            is_first_rule = False

            # if apply condition is unlabeled device and at least some tiles are labeled stop the process
            if rule_apply_condition == self.UNLABELED_DEVICES:
                if labeled_tiles_df.filter(F.col("is_labeled")).count() > 0:
                    break

        labeled_tiles_df = reduce(DataFrame.unionAll, labeled_tiles_dfs_list)
        labeled_tiles_df = labeled_tiles_df.withColumn(ColNames.label, F.lit(label_code))

        return labeled_tiles_df

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
        ltps_df = ltps_df.filter(
            (F.col(ColNames.id_type) == UeGridIdType.GRID_STR) | (F.col(ColNames.id_type) == UeGridIdType.ABROAD_STR)
        )

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
        return pre_selected_tiles_df.select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)

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

    @staticmethod
    def calculate_device_abs_threshold(
        device_observation: DataFrame, target_rows_ltps_df: DataFrame, perc_threshold: float, threshold_col: str
    ) -> DataFrame:
        """
        Calculate the total assigned long-term metric score for each device (total_device_ps) for a given day type
        and time interval and add this information to an additional column of the provided dataset. Then, based on
        this column, generate the absolute metric threshold to consider for each device by applying the corresponding
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
        device_observation = device_observation.withColumnRenamed(threshold_col, "total_device_value")
        target_rows_ltps_df = (
            target_rows_ltps_df.join(device_observation, on=[ColNames.user_id_modulo, ColNames.user_id])
            .withColumn("abs_threshold", F.col("total_device_value") * F.lit(perc_threshold / 100))
            .drop("total_device_value")
        )

        return target_rows_ltps_df

    def compute_quality_metrics(self) -> DataFrame:
        """
        Build usual environment labeling quality metrics dataframe.

        Args:
            ue_labels_df (DataFrame): usual environment labels dataframe.

        Returns:
            DataFrame: quality metrics dataframe.
        """
        self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].read()
        labeled_tiles_df = self.output_data_objects[SilverUsualEnvironmentLabelsDataObject.ID].df
        labeled_tiles_df = labeled_tiles_df.filter(
            (F.col(ColNames.season) == self.season)
            & (F.col(ColNames.start_date) == self.start_date)
            & (F.col(ColNames.end_date) == self.end_date)
        ).select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id, ColNames.label, ColNames.label_rule)

        # To get number of tiles not labeled as ue and meanigful locations, we need to get count of all tiles per user
        all_tiles_df = self.ltps_df.filter(
            (F.col(ColNames.day_type) == PeriodCombinations.ALL_PERIODS[1])
            & (F.col(ColNames.time_interval) == PeriodCombinations.ALL_PERIODS[2])
            & (F.col(ColNames.id_type) != UeGridIdType.DEVICE_OBSERVATION_STR)
        ).select(ColNames.user_id_modulo, ColNames.user_id, ColNames.grid_id)

        all_tiles_count_df = (
            all_tiles_df.groupBy(ColNames.user_id_modulo, ColNames.user_id)
            .count()
            .withColumnRenamed("count", "all_tiles")
        )

        # Mark all ue and meaningful locations rules as loc_na and ue_na
        grouped_df_location = labeled_tiles_df.withColumn(
            ColNames.label_rule, F.when(F.col(ColNames.label) != "ue", "loc_na").otherwise("ue_na")
        )

        # Count number of tiles per user per ue and meaningful locations
        grouped_df_location = grouped_df_location.groupBy(ColNames.user_id_modulo, ColNames.user_id, "label_rule").agg(
            F.count_distinct(ColNames.grid_id).alias("location_count")
        )

        # Join all tiles count with location count to get count of tiles not labeled as ue and meaningful locations
        grouped_df_location = grouped_df_location.join(
            all_tiles_count_df, on=[ColNames.user_id_modulo, ColNames.user_id], how="outer"
        )
        grouped_df_location = grouped_df_location.withColumn(
            "count", F.col("all_tiles") - F.col("location_count")
        ).drop("all_tiles", "location_count")

        # Count labeled tiles for separate rules
        grouped_df = labeled_tiles_df.groupby(
            ColNames.user_id_modulo, ColNames.user_id, ColNames.label, ColNames.label_rule
        ).count()

        device_quality_metrics_df = grouped_df.drop("label").unionByName(grouped_df_location)

        device_quality_metrics_df = device_quality_metrics_df.groupBy(ColNames.label_rule).agg(
            F.sum("count").alias(ColNames.labeling_quality_count),
            F.min("count").alias(ColNames.labeling_quality_min),
            F.max("count").alias(ColNames.labeling_quality_max),
            F.avg("count").alias(ColNames.labeling_quality_avg),
        )

        # Add device observation and ue abroad metrics
        abroad_ue_count = (
            labeled_tiles_df.filter(F.col(ColNames.id_type) == UeGridIdType.ABROAD_STR)
            .select(ColNames.user_id)
            .distinct()
            .count()
        )
        other_rule_metrics_df = self.spark.createDataFrame(
            [
                ("device_filter_1", self.rare_devices_count, 0, 0, 0),
                ("device_filter_2", self.discontinuous_devices_count, 0, 0, 0),
                ("ue_abroad", abroad_ue_count, 0, 0, 0),
            ],
            [
                ColNames.label_rule,
                ColNames.labeling_quality_count,
                ColNames.labeling_quality_min,
                ColNames.labeling_quality_max,
                ColNames.labeling_quality_avg,
            ],
        )

        device_quality_metrics_df = device_quality_metrics_df.unionByName(other_rule_metrics_df)

        device_quality_metrics_df = device_quality_metrics_df.withColumns(
            {ColNames.start_date: F.lit(self.start_date), ColNames.end_date: F.lit(self.end_date)}
        ).withColumnRenamed(ColNames.label_rule, ColNames.labeling_quality_metric)

        device_quality_metrics_df = apply_schema_casting(
            device_quality_metrics_df, SilverUsualEnvironmentLabelingQualityMetricsDataObject.SCHEMA
        )

        return device_quality_metrics_df

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
