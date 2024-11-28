""" 
Module for calculating which cells have overlapping coverage areas, as well calculating distances between coverage areas.
"""

from datetime import datetime, timedelta
from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_distance_data_object import SilverCellDistanceDataObject
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.grid import InspireGridGenerator
from multimno.core.log import get_execution_stats
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.spark_session import delete_file_or_folder
import pyspark.sql.functions as F
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP
from pyspark.sql.types import ShortType, ByteType, FloatType
from pyspark.sql import DataFrame


class CellProximityEstimation(Component):
    COMPONENT_ID = "CellProximityEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.max_nearby_cell_distance_m = self.config.getfloat(self.COMPONENT_ID, "max_nearby_cell_distance_m")
        self.footprint_buffer_distance_m = self.config.getfloat(self.COMPONENT_ID, "footprint_buffer_distance_m")
        self.n_output_partitions = self.config.getint(self.COMPONENT_ID, "n_output_partitions")
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
        self.grid_gen = InspireGridGenerator(self.spark)

        self.current_date = None
        self.prev_date = None

    def initalize_data_objects(self):
        # Input paths
        input_cell_footprint_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
        # Output paths
        output_cell_intersection_groups_silver_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "cell_intersection_groups_data_silver"
        )
        output_cell_distance_silver_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_distance_data_silver")
        clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")

        # Input data objects
        silver_cell_footprint = SilverCellFootprintDataObject(self.spark, input_cell_footprint_silver_path)
        self.input_data_objects = {SilverCellFootprintDataObject.ID: silver_cell_footprint}
        # Output data objects
        silver_cell_intersection_groups = SilverCellIntersectionGroupsDataObject(
            self.spark, output_cell_intersection_groups_silver_path
        )
        silver_cell_distance = SilverCellDistanceDataObject(self.spark, output_cell_distance_silver_path)
        self.output_data_objects = {
            SilverCellIntersectionGroupsDataObject.ID: silver_cell_intersection_groups,
            SilverCellDistanceDataObject.ID: silver_cell_distance,
        }

        if clear_destination_directory:
            delete_file_or_folder(self.spark, output_cell_intersection_groups_silver_path)
            delete_file_or_folder(self.spark, output_cell_distance_silver_path)

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        for current_date in self.data_period_dates:
            self.current_date = current_date
            self.transform()
            self.write()
            self.prev_date = current_date
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        current_date = self.current_date
        self.logger.info(f"Starting Transform method of {self.COMPONENT_ID} for date {current_date} ...")
        # Get cell grid_ids for current date.
        cell_grid_ids_df = (
            self.input_data_objects[SilverCellFootprintDataObject.ID]
            .df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            )
            .select(ColNames.cell_id, ColNames.grid_id)
        )
        # Shortcut to next date if no data is present.
        if cell_grid_ids_df.isEmpty():
            self.logger.info(f"No input data for date {current_date}.")
            self.output_data_objects[SilverCellIntersectionGroupsDataObject.ID].df = self.output_data_objects[
                SilverCellIntersectionGroupsDataObject.ID
            ].df.limit(0)
            self.output_data_objects[SilverCellDistanceDataObject.ID].df = self.output_data_objects[
                SilverCellDistanceDataObject.ID
            ].df.limit(0)
            return

        # Aggregate and sort list of grid ids per cell.
        df = cell_grid_ids_df.groupBy(ColNames.cell_id).agg(
            F.array_sort(F.collect_list(ColNames.grid_id)).alias("grid_id_list")
        )
        # For each cell, calculate the geometry from grid (concave hull of grid centroids, with buffer zone).
        df_with_geom = self.calculate_cell_geometry_with_buffer(df)

        # Perform dataframe self join to get cell pairs where distance is below max distance threshold.
        df = self.calculate_nearby_cell_pairs(df_with_geom)
        # Zero distance between two geoms indicates an overlap.
        # We currently do not account for the amount of overlap, simply its existence.
        df = df.withColumn("is_overlap", F.col(ColNames.distance) <= 0.0)
        df = df.cache()

        # Collect overlapping cell ids for output
        cell_intersection_groups_df = self.create_cell_intersection_groups_dataframe(df, current_date=current_date)
        cell_intersection_groups_df = cell_intersection_groups_df.repartition(
            self.n_output_partitions, ColNames.cell_id
        )
        self.output_data_objects[SilverCellIntersectionGroupsDataObject.ID].df = cell_intersection_groups_df

        # Collect cell distances for output
        cell_distance_df = self.create_cell_distance_dataframe(df, current_date=current_date)
        cell_distance_df = cell_distance_df.repartition(self.n_output_partitions, ColNames.cell_id_a)
        self.output_data_objects[SilverCellDistanceDataObject.ID].df = cell_distance_df
        return

    def create_cell_distance_dataframe(self, df: DataFrame, current_date) -> DataFrame:
        """Creates dataframe matching the cell distance data object structure.

        Args:
            df (DataFrame): (cell_id_a, cell_id_b, distance)
            current_date: date of data

        Returns:
            DataFrame: (cell_id_a ,cell_id_b, distance, year, month, day)
        """
        cell_distance_df = df.select(
            ColNames.cell_id_a,
            ColNames.cell_id_b,
            F.col(ColNames.distance).cast(FloatType()),
            F.lit(current_date.year).cast(ShortType()).alias(ColNames.year),
            F.lit(current_date.month).cast(ByteType()).alias(ColNames.month),
            F.lit(current_date.day).cast(ByteType()).alias(ColNames.day),
        ).where(F.col(ColNames.cell_id_a) != F.col(ColNames.cell_id_b))

        return cell_distance_df

    def create_cell_intersection_groups_dataframe(self, df: DataFrame, current_date) -> DataFrame:
        """Creates dataframe matching the cell intersection groups data object structure.

        Args:
            df (DataFrame): (cell_id_a, cell_id_b, is_overlap)
            current_date: date of data

        Returns:
            DataFrame: (cell_id_a, overlapping_cell_ids, year, month, day)
        """
        cell_intersection_groups_df = (
            df.where("is_overlap")
            .groupBy(ColNames.cell_id_a)
            .agg(F.array_sort(F.collect_list(ColNames.cell_id_b)).alias(ColNames.overlapping_cell_ids))
        )
        cell_intersection_groups_df = cell_intersection_groups_df.withColumnRenamed(
            ColNames.cell_id_a, ColNames.cell_id
        )
        cell_intersection_groups_df = cell_intersection_groups_df.select(
            ColNames.cell_id,
            ColNames.overlapping_cell_ids,
            F.lit(current_date.year).cast(ShortType()).alias(ColNames.year),
            F.lit(current_date.month).cast(ByteType()).alias(ColNames.month),
            F.lit(current_date.day).cast(ByteType()).alias(ColNames.day),
        )
        return cell_intersection_groups_df

    def calculate_nearby_cell_pairs(self, df: DataFrame) -> DataFrame:
        """Calculates pairs of cells in the dataframe which are within parameter-defined distance of each other.
        Pairs between the same cell_id have the cell_id_b set to None.
        Args:
            df (DataFrame): (cell_id, geometry)

        Returns:
            DataFrame: (cell_id_a, cell_id_b, distance)
        """
        df = (
            df.alias("df1")
            .join(
                df.alias("df2"),
                on=STP.ST_DWithin(
                    F.col(f"df1.{ColNames.geometry}"),
                    F.col(f"df2.{ColNames.geometry}"),
                    distance=self.max_nearby_cell_distance_m,
                ),
            )
            .select(
                F.col(f"df1.{ColNames.cell_id}").alias(ColNames.cell_id_a),
                F.when((F.col(f"df1.{ColNames.cell_id}") == F.col(f"df2.{ColNames.cell_id}")), F.lit(None))
                .otherwise(F.col(f"df2.{ColNames.cell_id}"))
                .alias(ColNames.cell_id_b),
                STF.ST_Distance(F.col(f"df1.{ColNames.geometry}"), F.col(f"df2.{ColNames.geometry}")).alias(
                    ColNames.distance
                ),
            )
        )
        return df

    def calculate_cell_geometry_with_buffer(self, df: DataFrame) -> DataFrame:
        """Calculates cell coverage geometries for cell rows, including added buffer zone.
        Returns (cell_id, geometry) for each row.

        Args:
            df (DataFrame): (cell_id, is_new_geometry, grid_id_list)

        Returns:
            DataFrame: (cell_id, geometry)
        """
        df_with_geom = df.select(ColNames.cell_id, F.explode("grid_id_list").alias(ColNames.grid_id))
        df_with_geom = self.grid_gen.grid_ids_to_centroids(df_with_geom)
        df_with_geom = df_with_geom.groupBy([ColNames.cell_id]).agg(
            STF.ST_ConcaveHull(STF.ST_Collect(F.collect_list(ColNames.geometry)), 0.5).alias(ColNames.geometry)
        )
        # Add buffer
        df_with_geom = df_with_geom.withColumn(
            ColNames.geometry, STF.ST_Buffer(ColNames.geometry, self.footprint_buffer_distance_m)
        )

        return df_with_geom
