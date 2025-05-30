"""
Module that implements the Daily Permanence Score functionality
"""

from datetime import datetime, timedelta
from multimno.core.spark_session import delete_file_or_folder, check_if_data_path_exists
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from sedona.sql import st_functions as STF

from multimno.core.component import Component

from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_cell_to_group_data_object import SilverCellToGroupDataObject
from multimno.core.data_objects.silver.silver_group_to_tile_data_object import SilverGroupToTileDataObject

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.log import get_execution_stats
from multimno.core.utils import apply_schema_casting


class CellFootprintIntersections(Component):
    """
    A class to calculate the daily permanence score of each user per interval and grid tile.
    """

    COMPONENT_ID = "CellFootprintIntersections"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        data_period_start = self.config.get(self.COMPONENT_ID, "data_period_start")
        try:
            self.data_period_start = datetime.strptime(data_period_start, "%Y-%m-%d").date()
        except ValueError as e:
            self.logger.error(f"Could not parse data_period_start = `{data_period_start}`. Expected format: YYYY-MM")
            raise e

        data_period_end = self.config.get(self.COMPONENT_ID, "data_period_end")
        try:
            self.data_period_end = datetime.strptime(data_period_end, "%Y-%m-%d").date()
        except ValueError as e:
            self.logger.error(f"Could not parse data_period_end = `{data_period_end}`. Expected format: YYYY-MM")
            raise e

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        self.current_date: datetime.date = None

    def initalize_data_objects(self):
        # Get paths
        input_cell_footprint_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        cell_to_group_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_to_group_data_silver")
        group_to_tile_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "group_to_tile_data_silver")

        # Clear destination directory if needed
        clear_destination_directory = self.config.getboolean(
            self.COMPONENT_ID, "clear_destination_directory", fallback=False
        )
        if clear_destination_directory:
            self.logger.warning(f"Deleting: {cell_to_group_path}")
            delete_file_or_folder(self.spark, cell_to_group_path)

            self.logger.warning(f"Deleting: {group_to_tile_path}")
            delete_file_or_folder(self.spark, group_to_tile_path)

        cell_footprint = SilverCellFootprintDataObject(self.spark, input_cell_footprint_path)
        input_group_to_tile = SilverGroupToTileDataObject(self.spark, group_to_tile_path)

        cell_to_group = SilverCellToGroupDataObject(self.spark, cell_to_group_path)
        group_to_tile = SilverGroupToTileDataObject(self.spark, group_to_tile_path)

        # Check if there already exists a previous group_to_tile dataset. If not, initialise
        # an empty dataset
        if not check_if_data_path_exists(self.spark, group_to_tile_path):
            input_group_to_tile.df = self.spark.createDataFrame([], schema=SilverGroupToTileDataObject.SCHEMA)
            input_group_to_tile.write()

        self.input_data_objects = {
            cell_footprint.ID: cell_footprint,
            input_group_to_tile.ID: input_group_to_tile,
        }

        self.output_data_objects = {
            cell_to_group.ID: cell_to_group,
            group_to_tile.ID: group_to_tile,
        }

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        for current_date in self.data_period_dates:
            self.read()
            self.current_date = current_date
            self.logger.info(f"Processing cell footprint for {current_date.strftime('%Y-%m-%d')}")
            self.transform()
            self.write()
            self.logger.info(f"... finished with {current_date.strftime('%Y-%m-%d')}")

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        cell_footprint = (
            self.input_data_objects[SilverCellFootprintDataObject.ID]
            .df.filter(
                (F.col(ColNames.year) == F.lit(self.current_date.year))
                & (F.col(ColNames.month) == F.lit(self.current_date.month))
                & (F.col(ColNames.day) == F.lit(self.current_date.day))
            )
            .select(ColNames.grid_id, ColNames.cell_id)
        )

        existing_group_to_tile = self.input_data_objects[SilverGroupToTileDataObject.ID].df

        # First, we get the list of cells that provide coverage to a particular grid tile
        # We get this by getting all cells that contain a given tile in their footprint

        cell_groups_per_tile = (
            cell_footprint.groupBy(ColNames.grid_id)
            # We assume no repetitions, hence F.collect_list
            # Sorting the array is necessary as it will be used as a grouping key in the window below
            .agg(F.array_sort(F.collect_list(ColNames.cell_id)).alias(ColNames.cell_id))
        )

        # Now we are going to create the ID for the cell footprint intersection groups. This ID will be equal to
        # the md5 of the comma-separated string of sorted grid IDs that are covered by a specific list of cells.
        w = (
            Window.partitionBy(ColNames.cell_id)
            .orderBy(ColNames.grid_id)
            .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        cell_groups_per_tile = cell_groups_per_tile.withColumn(
            ColNames.group_id, F.md5(F.array_join(F.array_sort(F.collect_list(ColNames.grid_id).over(w)), ","))
        )

        # Persist, as we will write two datasets from this dataframe
        cell_groups_per_tile.persist()

        group_to_tile = cell_groups_per_tile.select(
            ColNames.group_id, ColNames.grid_id
        ).join(  # we don't need the list of cells  # keep only unseen group IDs
            existing_group_to_tile, on=ColNames.group_id, how="anti"
        )

        cell_to_group = (
            cell_groups_per_tile.select(ColNames.cell_id, ColNames.group_id)
            # we need distinct rows
            .distinct()
            .withColumn(ColNames.cell_id, F.explode(ColNames.cell_id))
            .withColumns(
                {
                    ColNames.year: F.lit(self.current_date.year),
                    ColNames.month: F.lit(self.current_date.month),
                    ColNames.day: F.lit(self.current_date.day),
                }
            )
        )

        self.output_data_objects[SilverGroupToTileDataObject.ID].df = apply_schema_casting(
            group_to_tile, SilverGroupToTileDataObject.SCHEMA
        )
        self.output_data_objects[SilverCellToGroupDataObject.ID].df = apply_schema_casting(
            cell_to_group, SilverCellToGroupDataObject.SCHEMA
        )
