"""
Module that computes quality metrics and warnings of the DailyPermanenceScore component
"""

import datetime as dt

from multimno.core.exceptions import CriticalQualityWarningRaisedException
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import UeGridIdType
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.utils import apply_schema_casting

from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_daily_permanence_score_quality_metrics_data_object import (
    SilverDailyPermanenceScoreQualityMetrics,
)


class DailyPermanenceScoreQualityMetrics(Component):
    """
    Class responsible for computing quality metrics on the daily permanence score.
    """

    COMPONENT_ID = "DailyPermanenceScoreQualityMetrics"

    def __init__(self, general_config_path, component_config_path):
        super().__init__(general_config_path, component_config_path)

        self.timestamp = dt.datetime.now()

        self.data_period_start = dt.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = dt.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.data_period_dates = [
            self.data_period_start + dt.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        self.unknown_intervals_pct_threshold = self.config.getfloat(
            self.COMPONENT_ID, "unknown_intervals_pct_threshold"
        )
        self.non_crit_unknown_devices_pct_threshold = self.config.getfloat(
            self.COMPONENT_ID, "non_crit_unknown_devices_pct_threshold"
        )
        self.crit_unknown_devices_pct_threshold = self.config.getfloat(
            self.COMPONENT_ID, "crit_unknown_devices_pct_threshold"
        )

        if self.unknown_intervals_pct_threshold < 0 or self.unknown_intervals_pct_threshold > 100:
            raise ValueError(
                f"Threshold `unknown_intervals_pct_threshold` must be a float between 0 and 100 -- found {self.unknown_intervals_pct_threshold}"
            )

        if self.non_crit_unknown_devices_pct_threshold < 0 or self.non_crit_unknown_devices_pct_threshold > 100:
            raise ValueError(
                f"Threshold `non_crit_unknown_devices_pct_threshold` must be a float between 0 and 100 -- found {self.non_crit_unknown_devices_pct_threshold}"
            )

        if self.crit_unknown_devices_pct_threshold < 0 or self.crit_unknown_devices_pct_threshold > 100:
            raise ValueError(
                f"Threshold `crit_cell_pct_threshold` must be a float between 0 and 100 -- found {self.crit_unknown_devices_pct_threshold}"
            )

        if self.non_crit_unknown_devices_pct_threshold > self.crit_unknown_devices_pct_threshold:
            raise ValueError(
                f"Non-critical warning threshold {self.non_crit_unknown_devices_pct_threshold} % should be lower than critical threshold {self.crit_unknown_devices_pct_threshold} %"
            )

        self.current_date: dt.date = None
        self.current_dps: DataFrame = None
        self.warnings: dict = {}

    def initalize_data_objects(self):
        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")

        input_dps_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_data_silver")

        self.input_data_objects = {}
        if check_if_data_path_exists(self.spark, input_dps_path):
            self.input_data_objects[SilverDailyPermanenceScoreDataObject.ID] = SilverDailyPermanenceScoreDataObject(
                self.spark, input_dps_path
            )
        else:
            self.logger.warning(f"Expected path {input_dps_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverDailyPermanenceScoreDataObject.ID}: {input_dps_path}")

        self.output_data_objects = {}
        output_metrics_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_quality_metrics")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, output_metrics_path)

        self.output_data_objects[SilverDailyPermanenceScoreQualityMetrics.ID] = (
            SilverDailyPermanenceScoreQualityMetrics(self.spark, output_metrics_path)
        )

    def transform(self):
        total_devices = self.current_dps.select(ColNames.user_id).distinct().count()

        # Find out the size of the time interval
        time_row = self.current_dps.select(ColNames.time_slot_initial_time, ColNames.time_slot_end_time).take(1)[0]
        initial_time = time_row[ColNames.time_slot_initial_time]
        end_time = time_row[ColNames.time_slot_end_time]

        day_time_slots = round(dt.timedelta(days=1).total_seconds() / (end_time - initial_time).total_seconds())

        # Find out the number of devices with too many unknown time intervals
        unknown_devices = (
            self.current_dps.filter(F.col(ColNames.id_type) == F.lit(UeGridIdType.UKNOWN_STR))  # unknown timeslots
            .groupBy(ColNames.user_id_modulo, ColNames.user_id)
            .agg((F.count("*") * F.lit(100 / day_time_slots)).alias("pct_unknown_timeslots"))  # pct of unknown slots
            .filter(F.col("pct_unknown_timeslots") >= F.lit(self.unknown_intervals_pct_threshold))  # filter
            .count()  # count number of devices
        )

        pct_unknown_devices = 100.0 * unknown_devices / total_devices

        apply_schema_casting
        qm_data = self.spark.createDataFrame(
            [
                Row(
                    **{
                        ColNames.result_timestamp: self.timestamp,
                        ColNames.num_unknown_devices: unknown_devices,
                        ColNames.pct_unknown_devices: pct_unknown_devices,
                        ColNames.month: self.current_date.month,
                        ColNames.day: self.current_date.day,
                        ColNames.year: self.current_date.year,
                    }
                )
            ],
            schema=SilverDailyPermanenceScoreQualityMetrics.SCHEMA,
        )

        qm_data = apply_schema_casting(qm_data, SilverDailyPermanenceScoreQualityMetrics.SCHEMA)

        self.warnings[self.current_date] = {"metric": pct_unknown_devices}

        if pct_unknown_devices >= self.crit_unknown_devices_pct_threshold:
            self.warnings[self.current_date]["warning"] = "critical"
        elif pct_unknown_devices > self.non_crit_unknown_devices_pct_threshold:
            self.warnings[self.current_date]["warning"] = "non_critical"
        else:
            self.warnings[self.current_date]["warning"] = "no"

        self.output_data_objects[SilverDailyPermanenceScoreQualityMetrics.ID].df = qm_data

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        self.warnings = {}

        for current_date in self.data_period_dates:
            self.logger.info(
                f"Computing Daily Permanence Score Quality Metrics for {current_date.strftime('%Y-%m-%d')}"
            )
            self.current_date = current_date

            self.current_dps = (
                self.input_data_objects[SilverDailyPermanenceScoreDataObject.ID]
                .df.select(
                    ColNames.user_id_modulo,
                    ColNames.user_id,
                    ColNames.id_type,
                    ColNames.time_slot_initial_time,
                    ColNames.time_slot_end_time,
                    ColNames.year,
                    ColNames.month,
                    ColNames.day,
                )
                .filter(
                    (F.col(ColNames.year) == F.lit(current_date.year))
                    & (F.col(ColNames.month) == F.lit(current_date.month))
                    & (F.col(ColNames.day) == F.lit(current_date.day))
                )
                .select(
                    ColNames.user_id_modulo,
                    ColNames.user_id,
                    ColNames.id_type,
                    ColNames.time_slot_initial_time,
                    ColNames.time_slot_end_time,
                )
            )

            current_dps_exists = len(self.current_dps.take(1)) > 0

            if not current_dps_exists:
                self.logger.warning(
                    f"No silver network data for {current_date} -- skipping quality metrics for this date"
                )
                continue

            self.transform()
            self.write()

        critical_warning = False
        for current_date in self.data_period_dates:
            metric = self.warnings[current_date]["metric"]
            warning = self.warnings[current_date]["warning"]

            if warning == "critical":
                critical_warning = True
                self.logger.warning(
                    f"Critical warning: Events of {current_date} in cells with no footprint represents {metric:.2f} % >= {self.crit_unknown_devices_pct_threshold} %"
                )
            elif warning == "non_critical":
                self.logger.warning(
                    f"Non-critical warning: Events of {current_date} in cells with no footprint represents {metric:.2f} % > {self.non_crit_unknown_devices_pct_threshold} %"
                )

        self.logger.info(f"Finished {self.COMPONENT_ID}")

        if critical_warning:
            raise CriticalQualityWarningRaisedException(self.COMPONENT_ID)
