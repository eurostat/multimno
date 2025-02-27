"""
Module that computes quality metrics and warnings of the CellFootPrintEstimation component
"""

import datetime as dt

from multimno.core.exceptions import CriticalQualityWarningRaisedException
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.utils import apply_schema_casting

from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_cell_footprint_quality_metrics_data_object import (
    SilverCellFootprintQualityMetrics,
)
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject


class CellFootPrintQualityMetrics(Component):
    """
    Class responsible for computing quality metrics on the cell footprint. In particular, it checks how many
    cells have no footprint assigned to them, computes the total events that make reference to each of those cells,
    and raises a noncritical or critical warning if these "lost" events represent a high percentage of the total
    events.
    """

    COMPONENT_ID = "CellFootPrintQualityMetrics"

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

        self.non_crit_event_pct_threshold = self.config.getfloat(self.COMPONENT_ID, "non_crit_event_pct_threshold")
        self.crit_event_pct_threshold = self.config.getfloat(self.COMPONENT_ID, "crit_event_pct_threshold")

        if self.non_crit_event_pct_threshold < 0 or self.non_crit_event_pct_threshold > 100:
            raise ValueError(
                f"Threshold `non_crit_event_pct_threshold` must be a float between 0 and 100 -- found {self.non_crit_event_pct_threshold}"
            )

        if self.crit_event_pct_threshold < 0 or self.crit_event_pct_threshold > 100:
            raise ValueError(
                f"Threshold `crit_event_pct_threshold` must be a float between 0 and 100 -- found {self.crit_event_pct_threshold}"
            )

        if self.non_crit_event_pct_threshold > self.crit_event_pct_threshold:
            raise ValueError(
                f"Non-critical warning threshold {self.non_crit_event_pct_threshold} % should be lower than critical threshold {self.crit_event_pct_threshold}"
            )

        self.non_crit_cell_pct_threshold = self.config.getfloat(self.COMPONENT_ID, "non_crit_cell_pct_threshold")
        self.crit_cell_pct_threshold = self.config.getfloat(self.COMPONENT_ID, "crit_cell_pct_threshold")

        if self.non_crit_cell_pct_threshold < 0 or self.non_crit_cell_pct_threshold > 100:
            raise ValueError(
                f"Threshold `non_crit_cell_pct_threshold` must be a float between 0 and 100 -- found {self.non_crit_cell_pct_threshold}"
            )

        if self.crit_event_pct_threshold < 0 or self.crit_event_pct_threshold > 100:
            raise ValueError(
                f"Threshold `crit_event_pct_threshold` must be a float between 0 and 100 -- found {self.crit_event_pct_threshold}"
            )

        if self.non_crit_cell_pct_threshold > self.crit_cell_pct_threshold:
            raise ValueError(
                f"Non-critical warning threshold {self.non_crit_cell_pct_threshold} % should be lower than critical threshold {self.crit_cell_pct_threshold}"
            )

        self.current_date: dt.date = None
        self.current_network: DataFrame = None
        self.current_cell_footprint: DataFrame = None
        self.current_events: DataFrame = None
        self.event_warnings: dict = {}
        self.cell_warnings: dict = {}

    def initalize_data_objects(self):
        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        input_network_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        input_cell_footprint_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        input_events_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver")

        self.input_data_objects = {}
        if check_if_data_path_exists(self.spark, input_network_path):
            self.input_data_objects[SilverNetworkDataObject.ID] = SilverCellFootprintDataObject(
                self.spark, input_network_path
            )
        else:
            self.logger.warning(f"Expected path {input_network_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverNetworkDataObject.ID}: {input_network_path}")

        if check_if_data_path_exists(self.spark, input_cell_footprint_path):
            self.input_data_objects[SilverCellFootprintDataObject.ID] = SilverCellFootprintDataObject(
                self.spark, input_cell_footprint_path
            )
        else:
            self.logger.warning(f"Expected path {input_cell_footprint_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverCellFootprintDataObject.ID}: {input_cell_footprint_path}")

        if check_if_data_path_exists(self.spark, input_events_path):
            self.input_data_objects[SilverEventDataObject.ID] = SilverEventDataObject(self.spark, input_events_path)
        else:
            self.logger.warning(f"Expected path {input_events_path} to exist but it does not")
            raise ValueError(f"Invalid path for {SilverEventDataObject.ID}: {input_events_path}")

        self.output_data_objects = {}
        output_metrics_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_quality_metrics")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, output_metrics_path)

        self.output_data_objects[SilverCellFootprintQualityMetrics.ID] = SilverCellFootprintQualityMetrics(
            self.spark, output_metrics_path
        )

    def transform(self):
        # Get cell IDs with no cell footprint
        no_footprint_cells = self.current_network.join(
            self.current_cell_footprint, on=ColNames.cell_id, how="left_anti"
        )
        # Get total number of events
        total_events = self.current_events.count()
        total_cells = self.current_network.count()

        # Get number of events related to cells with no footprint
        no_footprint_cells = (
            no_footprint_cells
            # `day` column will be NULL if some cell has zero events
            .join(self.current_events, on=ColNames.cell_id, how="left")
            .groupBy(ColNames.cell_id)
            .agg(F.count(ColNames.day).alias(ColNames.number_of_events))
            .withColumn(
                ColNames.percentage_total_events, F.lit(100.0 / total_events) * F.col(ColNames.number_of_events)
            )
        )

        # Add metadata columns
        no_footprint_cells = no_footprint_cells.withColumns(
            {
                ColNames.year: F.lit(self.current_date.year),
                ColNames.month: F.lit(self.current_date.month),
                ColNames.day: F.lit(self.current_date.day),
                ColNames.result_timestamp: F.lit(self.timestamp),
            }
        )

        no_footprint_cells = apply_schema_casting(no_footprint_cells, SilverCellFootprintQualityMetrics.SCHEMA)
        no_footprint_cells.cache()

        missed_cells_pct = 100 * no_footprint_cells.count() / total_cells

        missed_events_pct = no_footprint_cells.select(
            F.sum(ColNames.percentage_total_events).alias("total_pct")
        ).collect()[0]["total_pct"]
        missed_events_pct = missed_events_pct if missed_events_pct is not None else 0

        self.cell_warnings[self.current_date] = {"metric": missed_cells_pct}
        self.event_warnings[self.current_date] = {"metric": missed_events_pct}

        # Event warnings
        if missed_events_pct >= self.crit_event_pct_threshold:
            self.event_warnings[self.current_date]["warning"] = "critical"
        elif missed_events_pct > self.non_crit_event_pct_threshold:
            self.event_warnings[self.current_date]["warning"] = "non_critical"
        else:
            self.event_warnings[self.current_date]["warning"] = "no"

        # Cell warnings
        if missed_cells_pct >= self.crit_cell_pct_threshold:
            self.cell_warnings[self.current_date]["warning"] = "critical"
        elif missed_cells_pct > self.non_crit_cell_pct_threshold:
            self.cell_warnings[self.current_date]["warning"] = "non_critical"
        else:
            self.cell_warnings[self.current_date]["warning"] = "no"

        self.output_data_objects[SilverCellFootprintQualityMetrics.ID].df = no_footprint_cells

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        self.warnings = {}

        for current_date in self.data_period_dates:
            self.logger.info(f"Computing Cell Footprint Quality Metrics for {current_date.strftime('%Y-%m-%d')}")
            self.current_date = current_date

            self.current_network = (
                self.input_data_objects[SilverNetworkDataObject.ID]
                .df.select(ColNames.cell_id, ColNames.year, ColNames.month, ColNames.day)
                .filter(
                    (F.col(ColNames.year) == F.lit(current_date.year))
                    & (F.col(ColNames.month) == F.lit(current_date.month))
                    & (F.col(ColNames.day) == F.lit(current_date.day))
                )
                .select(ColNames.cell_id)
            )

            self.current_cell_footprint = (
                self.input_data_objects[SilverCellFootprintDataObject.ID]
                .df.select(ColNames.cell_id, ColNames.grid_id, ColNames.year, ColNames.month, ColNames.day)
                .filter(
                    (F.col(ColNames.year) == F.lit(current_date.year))
                    & (F.col(ColNames.month) == F.lit(current_date.month))
                    & (F.col(ColNames.day) == F.lit(current_date.day))
                )
                .select(ColNames.cell_id, ColNames.grid_id)
            )

            self.current_events = (
                self.input_data_objects[SilverEventDataObject.ID]
                .df.select(ColNames.cell_id, ColNames.year, ColNames.month, ColNames.day)
                .filter(
                    (F.col(ColNames.year) == F.lit(current_date.year))
                    & (F.col(ColNames.month) == F.lit(current_date.month))
                    & (F.col(ColNames.day) == F.lit(current_date.day))
                )
                # we keep `day` column for later counting of events on cells
                .select(ColNames.cell_id, ColNames.day)
            )

            current_network_exists = len(self.current_network.take(1)) > 0
            current_footprint_exists = len(self.current_cell_footprint.take(1)) > 0
            current_event_exists = len(self.current_events.take(1)) > 0

            if not current_network_exists:
                self.logger.warning(
                    f"No silver network data for {current_date} -- skipping quality metrics for this date"
                )
                continue

            if not current_footprint_exists:
                self.logger.warning(
                    f"No cell footprint data for {current_date} -- skipping quality metrics for this date"
                )
                continue

            if not current_event_exists:
                self.logger.warning(
                    f"No silver event data for {current_date} -- skipping quality metrics for this date"
                )
                continue

            self.transform()
            self.write()

        critical_warning = False
        for current_date in self.data_period_dates:
            event_metric = self.event_warnings[current_date]["metric"]
            cell_metric = self.cell_warnings[current_date]["metric"]
            event_warning = self.event_warnings[current_date]["warning"]
            cell_warning = self.cell_warnings[current_date]["warning"]

            if event_warning == "critical":
                critical_warning = True
                self.logger.warning(
                    f"Critical warning: Events of {current_date} in cells with no footprint represents {event_metric:.2f} % >= {self.crit_event_pct_threshold} %"
                )
            elif event_warning == "non_critical":
                self.logger.warning(
                    f"Non-critical warning: Events of {current_date} in cells with no footprint represents {event_metric:.2f} % > {self.non_crit_event_pct_threshold} %"
                )

            if cell_warning == "critical":
                critical_warning = True
                self.logger.warning(
                    f"Critical warning: Cells of {current_date} with no footprint represents {cell_metric:.2f} % >= {self.crit_cell_pct_threshold} %"
                )
            elif cell_warning == "non_critical":
                self.logger.warning(
                    f"Non-critical warning: Cells of {current_date} with no footprint represents {cell_metric:.2f} % > {self.non_crit_cell_pct_threshold} %"
                )

        self.logger.info(f"Finished {self.COMPONENT_ID}")

        if critical_warning:
            raise CriticalQualityWarningRaisedException(self.COMPONENT_ID)
