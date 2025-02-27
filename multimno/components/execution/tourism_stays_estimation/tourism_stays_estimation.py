"""
Module that implements the Tourism Stays Estimation functionality.
"""

from datetime import datetime, timedelta

from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import SilverGeozonesGridMapDataObject
from multimno.core.data_objects.silver.silver_tourism_stays_data_object import SilverTourismStaysDataObject
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.grid import InspireGridGenerator
import pyspark.sql.functions as F

from multimno.core.component import Component

from multimno.core.data_objects.silver.silver_time_segments_data_object import (
    SilverTimeSegmentsDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, GENERAL_CONFIG_KEY
from multimno.core.constants.columns import ColNames, SegmentStates
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


class TourismStaysEstimation(Component):
    """
    A class to calculate geozones for inbound time segments.
    """

    COMPONENT_ID = "TourismStaysEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        # get state codes from string
        self.segment_states_to_include = ["stay"]
        # Convert string states to indices
        self.segment_states_to_include = [SegmentStates.STR_TO_INDEX[state] for state in self.segment_states_to_include]

        self.local_mcc = self.config.getint(GENERAL_CONFIG_KEY, "local_mcc")
        self.zoning_dataset_ids_list = self.config.geteval(self.COMPONENT_ID, "zoning_dataset_ids_list")
        self.min_duration_segment_m = self.config.geteval(self.COMPONENT_ID, "min_duration_segment_m")
        self.functional_midnight_h = self.config.geteval(self.COMPONENT_ID, "functional_midnight_h")
        self.min_duration_segment_night_m = self.config.geteval(self.COMPONENT_ID, "min_duration_segment_night_m")

    def initalize_data_objects(self):

        self.filter_ue_segments = self.config.getboolean(self.COMPONENT_ID, "filter_ue_segments")

        # Input
        self.input_data_objects = {}

        inputs = {
            "time_segments_silver": SilverTimeSegmentsDataObject,
            "cell_connection_probabilities_data_silver": SilverCellConnectionProbabilitiesDataObject,
            "geozones_grid_map_data_silver": SilverGeozonesGridMapDataObject,
        }

        # If releveant, add usual environment labels data object to filter all devices that have a usual environment label
        if self.filter_ue_segments:
            inputs["usual_environment_labels_data_silver"] = SilverUsualEnvironmentLabelsDataObject

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # Output
        self.output_data_objects = {}
        self.output_daily_silver_tourism_stays_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "tourism_stays_silver")
        self.output_data_objects[SilverTourismStaysDataObject.ID] = SilverTourismStaysDataObject(
            self.spark,
            self.output_daily_silver_tourism_stays_path,
        )

        # Output clearing
        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        if clear_destination_directory:
            delete_file_or_folder(self.spark, self.output_daily_silver_tourism_stays_path)

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        # for each zoning system, for every date in the data period, process segments and map them to zone ids
        for current_zoning_dataset_id in self.zoning_dataset_ids_list:
            self.current_zoning_dataset_id = current_zoning_dataset_id
            # Handle reserved zoning dataset inputs
            if current_zoning_dataset_id in ReservedDatasetIDs():
                self.current_geozones_grid_mapping_df = None
            else:
                self.current_geozones_grid_mapping_df = self.input_data_objects[
                    SilverGeozonesGridMapDataObject.ID
                ].df.filter(F.col(ColNames.dataset_id) == F.lit(current_zoning_dataset_id))

            for current_date in self.data_period_dates:
                self.logger.info(
                    f"Processing events for {current_date.strftime('%Y-%m-%d')}, zoning dataset {current_zoning_dataset_id}"
                )

                # get list of inbound roamers from usual environment labels
                if self.filter_ue_segments:
                    inbound_residents_df = (
                        self.input_data_objects[SilverUsualEnvironmentLabelsDataObject.ID]
                        .df.filter(
                            ((F.col(ColNames.label) == "ue"))
                            & (F.col(ColNames.start_date) <= current_date)
                            & (F.col(ColNames.end_date) >= current_date)
                        )
                        .select(ColNames.user_id)
                        .distinct()
                    )

                self.current_date = current_date
                self.current_time_segments_df = self.input_data_objects[SilverTimeSegmentsDataObject.ID].df.filter(
                    (
                        F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                        == F.lit(current_date)
                    )
                    & (F.col(ColNames.state).isin(self.segment_states_to_include))
                    & (F.col(ColNames.mcc) != self.local_mcc)
                    & (
                        (F.col(ColNames.end_timestamp).cast("long") - F.col(ColNames.start_timestamp).cast("long"))
                        / 60.0
                        >= self.min_duration_segment_m
                    )
                )

                # filter out segments that are inbound residents
                if self.filter_ue_segments:
                    self.current_time_segments_df = self.current_time_segments_df.join(
                        inbound_residents_df, on=ColNames.user_id, how="left_anti"
                    )

                self.current_cell_connection_probabilities_df = self.input_data_objects[
                    SilverCellConnectionProbabilitiesDataObject.ID
                ].df.filter(
                    (
                        F.make_date(
                            F.col(ColNames.year),
                            F.col(ColNames.month),
                            F.col(ColNames.day),
                        )
                        == F.lit(current_date)
                    )
                )

                self.transform()
                self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        current_time_segments_df = self.current_time_segments_df
        current_cell_connection_probabilities_df = self.current_cell_connection_probabilities_df
        current_geozones_grid_mapping_df = self.current_geozones_grid_mapping_df
        current_zoning_dataset_id = self.current_zoning_dataset_id

        # TODO cache current_time_segments_df?
        # Extract unique cells arrays among segments.
        unique_cells_df = current_time_segments_df.select(F.col(ColNames.cells)).distinct()

        # For each unique cells array, join to cell-grid dataframe to get grids with posterior probabilities.
        # Normalize grid probabilities (divide probabilities by cell count) to sum to 1.
        cells_arr_grid_prob_df = (
            unique_cells_df.withColumn(ColNames.cell_id, F.explode(F.col(ColNames.cells)))
            .alias("df1")
            .join(
                current_cell_connection_probabilities_df.alias("df2"),
                on=ColNames.cell_id,
                how="inner",
            )
            .select(
                "df1.*",
                f"df2.{ColNames.grid_id}",
                (F.col(f"df2.{ColNames.posterior_probability}") / F.size(f"df1.{ColNames.cells}")).alias("grid_weight"),
            )
        )

        # For each unique cells array, map to the lowest level of zone hierarchy and calculate zone weight as sum of grid weights in that zone.
        # For reserved zoning datasets, instead use grid generator to map grid id to desired resolution and use its string form instead of hierarhical id.
        if current_zoning_dataset_id in ReservedDatasetIDs():
            resolution = ReservedDatasetIDs.get_resolution_m(current_zoning_dataset_id)
            inspire_grid_generator = InspireGridGenerator(spark=self.spark)
            cells_arr_zone_df = (
                inspire_grid_generator.get_parent_grid_ids(
                    sdf=cells_arr_grid_prob_df, resolution=resolution, parent_col_name=ColNames.hierarchical_id
                )
                .groupBy(ColNames.cells, ColNames.hierarchical_id)
                .agg(
                    F.sum(F.col("grid_weight")).cast("float").alias(ColNames.zone_weight),
                )
            ).withColumn(ColNames.dataset_id, F.lit(current_zoning_dataset_id))
            cells_arr_zone_df = inspire_grid_generator.convert_internal_id_to_inspire_specs(
                sdf=cells_arr_zone_df, resolution=resolution, grid_id_col=ColNames.hierarchical_id
            )
        else:
            cells_arr_zone_df = (
                cells_arr_grid_prob_df.alias("df1")
                .join(current_geozones_grid_mapping_df, on=ColNames.grid_id, how="inner")
                .groupBy(ColNames.cells, ColNames.hierarchical_id)
                .agg(
                    F.sum(F.col("grid_weight")).cast("float").alias(ColNames.zone_weight),
                )
            ).withColumn(ColNames.dataset_id, F.lit(current_zoning_dataset_id))

        # Join unique cells array with zone mappings back to the original segments.
        segments_with_zone_weights_df = (
            current_time_segments_df.alias("df1")
            .join(cells_arr_zone_df.alias("df2"), on=ColNames.cells, how="inner")
            .select(
                "df1.*",
                ColNames.hierarchical_id,
                ColNames.zone_weight,
                ColNames.dataset_id,
            )
        )

        # Repartition and sort
        segments_with_zone_weights_df = segments_with_zone_weights_df.repartition(
            *SilverTourismStaysDataObject.PARTITION_COLUMNS
        ).sortWithinPartitions(ColNames.start_timestamp, ColNames.hierarchical_id)

        # Mark segments that contain the functional midnight hour and are sufficiently long as overnight segments.
        segments_with_zone_weights_df = segments_with_zone_weights_df.withColumn(
            ColNames.is_overnight,
            F.when(
                (F.hour(F.col(f"df1.{ColNames.start_timestamp}")) <= self.functional_midnight_h)
                & (F.hour(F.col(f"df1.{ColNames.end_timestamp}")) > self.functional_midnight_h)
                & (
                    ((F.col(ColNames.end_timestamp).cast("long") - F.col(ColNames.start_timestamp).cast("long")) / 60.0)
                    >= self.min_duration_segment_night_m
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        # Keep one row per segment, aggregating zone ids and weights.
        segments_with_zone_weights_df = segments_with_zone_weights_df.groupBy(
            ColNames.user_id,
            ColNames.time_segment_id,
            ColNames.start_timestamp,
            ColNames.end_timestamp,
            ColNames.is_overnight,
            ColNames.mcc,
            ColNames.mnc,
            ColNames.plmn,
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id_modulo,
            ColNames.dataset_id,
        ).agg(  # TODO collect_list order is not deterministic
            F.collect_list(ColNames.hierarchical_id).alias(ColNames.zone_ids_list),
            F.collect_list(ColNames.zone_weight).alias(ColNames.zone_weights_list),
        )

        segments_with_zone_weights_df = apply_schema_casting(
            segments_with_zone_weights_df, SilverTourismStaysDataObject.SCHEMA
        )

        # Prepare output
        self.output_data_objects[SilverTourismStaysDataObject.ID].df = segments_with_zone_weights_df
