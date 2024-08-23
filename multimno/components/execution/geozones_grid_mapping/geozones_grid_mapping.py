"""
This module is responsible for enrichment of the operational grid with elevation and landuse data.
"""

from typing import Any, Dict
from sedona.sql import st_predicates as STP

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.core.component import Component

from multimno.core.data_objects.bronze.bronze_geographic_zones_data_object import (
    BronzeGeographicZonesDataObject,
)
from multimno.core.data_objects.bronze.bronze_admin_units_data_object import (
    BronzeAdminUnitsDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
    CONFIG_BRONZE_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats

class GeozonesGridMapping(Component):
    """
    This class is responsible for mapping of zoning data to the operational grid.
    """

    COMPONENT_ID = "GeozonesGridMapping"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.zoning_dataset_ids = self.config.geteval(GeozonesGridMapping.COMPONENT_ID, "dataset_ids")

    def initalize_data_objects(self):

        # inputs
        self.clear_destination_directory = self.config.getboolean(
            GeozonesGridMapping.COMPONENT_ID, "clear_destination_directory"
        )
        self.zoning_type = self.config.get(GeozonesGridMapping.COMPONENT_ID, "zoning_type")

        if self.zoning_type == "admin":
            zoning_data = {"admin_units_data_bronze": BronzeAdminUnitsDataObject}
        elif self.zoning_type == "other":
            zoning_data = {"geographic_zones_data_bronze": BronzeGeographicZonesDataObject}

        self.input_data_objects = {}
        inputs = {
            "grid_data_silver": SilverGridDataObject,
        } | zoning_data

        for key, value in inputs.items():
            if self.config.has_option(CONFIG_BRONZE_PATHS_KEY, key):
                path = self.config.get(CONFIG_BRONZE_PATHS_KEY, key)
            else:
                path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # outputs

        grid_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "geozones_grid_map_data_silver")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, grid_do_path)

        self.output_data_objects = {}
        self.output_data_objects[SilverGeozonesGridMapDataObject.ID] = SilverGeozonesGridMapDataObject(
            self.spark,
            grid_do_path,
            [
                ColNames.dataset_id,
                ColNames.year,
                ColNames.month,
                ColNames.day,
            ],
        )

    @get_execution_stats
    def execute(self):

        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        # iterate over each dataset_id and do mapping separately
        for dataset_id in self.zoning_dataset_ids:
            self.logger.info(f"Starting mapping for {dataset_id} dataset...")
            self.current_dataset_id = dataset_id
            self.read()
            self.transform()
            self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        current_zoning_sdf = self.input_data_objects[BronzeGeographicZonesDataObject.ID].df
        current_zoning_sdf = current_zoning_sdf.filter(
            current_zoning_sdf[ColNames.dataset_id].isin(self.current_dataset_id)
        )
        grid_sdf = self.input_data_objects[SilverGridDataObject.ID].df

        zoning_levels = self.get_hierarchy_levels(current_zoning_sdf)

        zone_grid_sdf = self.map_zoning_units_to_grid(grid_sdf, current_zoning_sdf, zoning_levels)

        zone_grid_sdf = self.extract_hierarchy_ids(zone_grid_sdf, current_zoning_sdf, zoning_levels)

        # get year, month, day from the current_zone_sdf year, month, day columns, assign to the zone_grid_sdf
        first_row = current_zoning_sdf.first()

        zone_grid_sdf = zone_grid_sdf.withColumn(ColNames.year, F.lit(first_row[ColNames.year]))
        zone_grid_sdf = zone_grid_sdf.withColumn(ColNames.month, F.lit(first_row[ColNames.month]))
        zone_grid_sdf = zone_grid_sdf.withColumn(ColNames.day, F.lit(first_row[ColNames.day]))

        zone_grid_sdf = zone_grid_sdf.withColumn(ColNames.dataset_id, F.lit(self.current_dataset_id))

        zone_grid_sdf = utils.apply_schema_casting(zone_grid_sdf, SilverGeozonesGridMapDataObject.SCHEMA)

        self.output_data_objects[SilverGeozonesGridMapDataObject.ID].df = zone_grid_sdf

    @staticmethod
    def get_hierarchy_levels(zone_units_df: DataFrame) -> Dict[str, int]:
        """
        Returns the distinct hierarchy levels of the zoning units in a sorted order.

        This method takes a DataFrame of zoning units, selects the distinct values in the 'level' column,
        and returns these levels in a sorted list.

        Args:
            zone_units_df (DataFrame): A DataFrame containing zoning units data.
                                    It is expected to have a column named 'level' which
                                    indicates the hierarchy level of each zoning unit.

        Returns:
            list: A sorted list of distinct hierarchy levels of the zoning units.
        """
        levels = [row[ColNames.level] for row in zone_units_df.select(ColNames.level).distinct().collect()]

        return sorted(levels)

    def map_zoning_units_to_grid(
        self, grid_sdf: DataFrame, zoning_units_df: DataFrame, zoning_levels: list
    ) -> DataFrame:
        """
        Maps zoning units to a grid.

        This method takes a DataFrame of grid data and a DataFrame of zoning units data,
        and maps the zoning units to the grid. The mapping is done based on the maximum zoning level.
        The method returns a DataFrame that contains the grid data with an additional column for the zoning unit ID.
        If a grid cell does not intersect with any zoning unit, the zoning unit ID for that cell is set to 'undefined'.

        Args:
            grid_sdf (DataFrame): A DataFrame containing grid data.
                                  It is expected to have a column named 'geometry' which indicates the geometry of each grid cell.
            zoning_units_df (DataFrame): A DataFrame containing zoning units data.
                                         It is expected to have columns named 'level' and 'geometry'
                                         which indicate the hierarchy level and the geometry of each zoning unit, respectively.
            zoning_levels (list): A list of zoning levels.

        Returns:
            DataFrame: A DataFrame that contains the grid data with an additional column for the zoning unit ID.
        """
        zoning_units_df = zoning_units_df.filter(F.col(ColNames.level) == max(zoning_levels))

        intersection_sdf = (
            grid_sdf.alias("a")
            .join(
                F.broadcast(zoning_units_df.alias("b")),
                STP.ST_Intersects(f"a.{ColNames.geometry}", f"b.{ColNames.geometry}"),
            )
            .select("a.*", f"b.{ColNames.zone_id}")
        )

        non_intersection_sdf = grid_sdf.alias("a").join(
            intersection_sdf.alias("b").select(
                ColNames.grid_id,
            ),
            F.col(f"a.{ColNames.grid_id}") == F.col(f"b.{ColNames.grid_id}"),
            "left_anti",
        )

        non_intersection_sdf = non_intersection_sdf.withColumn(ColNames.zone_id, F.lit("undefined"))

        lowest_zone_grid_sdf = intersection_sdf.union(non_intersection_sdf)

        return lowest_zone_grid_sdf

    def extract_hierarchy_ids(
        self, zone_grid_sdf: DataFrame, zone_units_sdf: DataFrame, zoning_levels: list
    ) -> DataFrame:
        """
        Extracts the hierarchy IDs for all zoning levels.

        This method takes a DataFrame of grid data mapped to zoning units and a DataFrame of zoning units data,
        and extracts the hierarchy IDs for all zoning levels. The hierarchy ID for a grid tile is a string that
        contains the IDs of the zoning units that the tile intersects with, ordered by the zoning level.

        Args:
            zone_grid_sdf (DataFrame): A DataFrame containing grid data mapped to zoning units.
                                       It is expected to have column named 'zone_id' which represents zone id on the lowest level.
            zone_units_sdf (DataFrame): A DataFrame containing zoning units data.
                                        It is expected to have columns named 'level', 'zone_id', and 'parent_id'.
            zoning_levels (int): The number of zoning levels.

        Returns:
            DataFrame: A DataFrame that contains the grid data with an additional column for the hierarchy ID.
        """

        for level in reversed(zoning_levels):

            current_level_zone_units_sdf = zone_units_sdf.filter(F.col(ColNames.level) == level)
            if level == max(zoning_levels):

                zone_grid_sdf = zone_grid_sdf.withColumn(ColNames.hierarchical_id, F.col(ColNames.zone_id))
                zone_grid_sdf = zone_grid_sdf.alias("a").join(
                    current_level_zone_units_sdf.alias("b"),
                    F.col(f"a.{ColNames.zone_id}") == F.col(f"b.{ColNames.zone_id}"),
                    "left",
                )
                zone_grid_sdf = zone_grid_sdf.withColumn("next_level_join_id", F.col(f"b.{ColNames.parent_id}"))

                zone_grid_sdf = zone_grid_sdf.select("a.*", "next_level_join_id")

            else:
                zone_grid_sdf = zone_grid_sdf.alias("a").join(
                    current_level_zone_units_sdf.alias("b"),
                    F.col(f"a.next_level_join_id") == F.col(f"b.{ColNames.zone_id}"),
                    "left",
                )

                zone_grid_sdf = zone_grid_sdf.withColumn(
                    ColNames.hierarchical_id,
                    F.when(
                        F.col(f"b.{ColNames.zone_id}").isNotNull(),
                        F.concat(
                            F.col(f"b.{ColNames.zone_id}"),
                            F.lit("|"),
                            F.col(f"{ColNames.hierarchical_id}"),
                        ),
                    ).otherwise(
                        F.concat(
                            F.lit("undefined"),
                            F.lit("|"),
                            F.col(f"a.{ColNames.hierarchical_id}"),
                        )
                    ),
                )
                zone_grid_sdf = zone_grid_sdf.withColumn("next_level_join_id", F.col(f"b.{ColNames.parent_id}"))
                zone_grid_sdf = zone_grid_sdf.select("a.*", "next_level_join_id", ColNames.hierarchical_id)

        return zone_grid_sdf
