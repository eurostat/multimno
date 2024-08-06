"""
This module is responsible for enrichment of the operational grid with elevation and landuse data.
"""

from functools import reduce
from typing import Any, Dict
import math
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP
from sedona.sql import st_aggregates as STA

import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType

from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_transportation_data_object import (
    BronzeTransportationDataObject,
)
from multimno.core.data_objects.bronze.bronze_landuse_data_object import (
    BronzeLanduseDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)

from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)

from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
    CONFIG_BRONZE_PATHS_KEY,
    CONFIG_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
from multimno.core.grid import InspireGridGenerator
import multimno.core.utils as utils


class GridEnrichment(Component):
    """
    This class is responsible for enrichment of the operational grid with elevation and landuse data.
    """

    COMPONENT_ID = "GridEnrichment"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.do_land_cover_enrichment = self.config.getboolean(GridEnrichment.COMPONENT_ID, "do_landcover_enrichment")
        self.transportation_category_buffer_m = self.config.geteval(
            GridEnrichment.COMPONENT_ID, "transportation_category_buffer_m"
        )
        self.prior_weights = self.config.geteval(GridEnrichment.COMPONENT_ID, "prior_weights")
        self.ple_coefficient_weights = self.config.geteval(GridEnrichment.COMPONENT_ID, "ple_coefficient_weights")
        # self.spatial_repartition_size_rows = self.config.getint(
        #     InspireGridGeneration.COMPONENT_ID, "spatial_repartition_size_rows"
        # )

        self.do_elevation_enrichment = self.config.getboolean(GridEnrichment.COMPONENT_ID, "do_elevation_enrichment")

        self.prior_calculation_repartition_size = self.config.getint(
            GridEnrichment.COMPONENT_ID, "prior_calculation_repartition_size"
        )

        self.quadkey_udf = F.udf(utils.latlon_to_quadkey)

        self.spark.sparkContext.setCheckpointDir(self.config.get(CONFIG_PATHS_KEY, "spark_checkpoint_dir"))

        # TODO: For now set to default 100, but can be dynamic from config in future
        self.grid_resolution = 100

        self.grid_generator = InspireGridGenerator(
            self.spark,
            self.grid_resolution,
            ColNames.geometry,
            ColNames.grid_id,
            1000,
        )

    def initalize_data_objects(self):

        # inputs
        self.clear_destination_directory = self.config.getboolean(
            GridEnrichment.COMPONENT_ID, "clear_destination_directory"
        )

        self.input_data_objects = {}
        inputs = {
            "grid_data_silver": SilverGridDataObject,
            "transportation_data_bronze": BronzeTransportationDataObject,
            "landuse_data_bronze": BronzeLanduseDataObject,
        }

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

        grid_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "enriched_grid_data_silver")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, grid_do_path)

        self.output_data_objects = {}
        self.output_data_objects[SilverEnrichedGridDataObject.ID] = SilverEnrichedGridDataObject(
            self.spark, grid_do_path, [ColNames.quadkey]
        )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        grid_sdf = self.input_data_objects[SilverGridDataObject.ID].df

        if self.do_elevation_enrichment:
            grid_sdf = self.add_elevation_to_grid()

        if self.do_land_cover_enrichment:

            # Depending on avaliable resources and data size of an area of interest
            # we might need to split computations into multiple parts using quadkey level
            grid_parts = []
            unique_quadkeys = grid_sdf.select(ColNames.quadkey).distinct().collect()
            self.logger.info(f"Calculating landuse prior...")
            self.logger.info(f"Unique quadkeys to process: {len(unique_quadkeys)}")
            for quadkey in unique_quadkeys:
                self.logger.info(f"Processing quadkey {quadkey[ColNames.quadkey]}")
                self.current_quadkey = quadkey[ColNames.quadkey]
                current_grid_part = grid_sdf.filter(F.col(ColNames.quadkey) == F.lit(self.current_quadkey))

                # map landuse and roads to grid tiles and get landuse ratios
                current_grid_part = self.map_landuse_to_grid(current_grid_part)

                # Calculate the weighted sums
                current_grid_part = self.calculated_landuse_ratios_weighted_sum(
                    current_grid_part, self.prior_weights, "weighted_sum"
                )

                # calculate PLE coefficient
                current_grid_part = self.calculated_landuse_ratios_weighted_sum(
                    current_grid_part,
                    self.ple_coefficient_weights,
                    ColNames.ple_coefficient,
                )

                # TODO: asses if it would be possible to use persist instead of checkpoint
                current_grid_part = current_grid_part.checkpoint()

                # clear the cache
                self.spark.catalog.clearCache()

                grid_parts.append(current_grid_part)

            grid_sdf = reduce(lambda x, y: x.union(y), grid_parts)
            grid_sdf = grid_sdf.dropDuplicates([ColNames.grid_id])

            grid_sdf = self.calculate_landuse_prior(grid_sdf)

        grid_sdf = self.grid_generator.grid_ids_to_centroids(grid_sdf)

        grid_sdf = grid_sdf.orderBy("quadkey")
        grid_sdf = grid_sdf.repartition("quadkey")

        # Cast column types to DO schema, add missing columns manually
        df_columns = set(grid_sdf.columns)
        schema_columns = set(field.name for field in SilverEnrichedGridDataObject.SCHEMA.fields)
        missing_columns = schema_columns - df_columns

        for column in missing_columns:
            grid_sdf = grid_sdf.withColumn(
                column,
                F.lit(None).cast(SilverEnrichedGridDataObject.SCHEMA[column].dataType),
            )

        grid_sdf = utils.apply_schema_casting(grid_sdf, SilverEnrichedGridDataObject.SCHEMA)

        self.output_data_objects[SilverEnrichedGridDataObject.ID].df = grid_sdf

    def add_elevation_to_grid(self):
        # TODO: implement elevation enrichment
        pass

    def map_landuse_to_grid(self, grid_sdf: DataFrame) -> DataFrame:
        """
        Maps land use and transportation data to a grid.

        This function takes a grid DataFrame, prepares the transportation and land use data
        by filtering and cutting it to the extent of the current quadkey,
        and then merges the prepared data with the grid. It also calculates the land use ratios per tile in the grid.

        Args:
            grid_sdf (pyspark.sql.DataFrame): The grid DataFrame to which the land use and transportation data will be mapped.

        Returns:
            pyspark.sql.DataFrame: The grid DataFrame with the mapped land use
            and transportation data as ratios of a total area.

        Raises:
            Warning: If no data is found for the current quadkey,
            a warning is logged and the function returns the grid DataFrame populated with empty ratios.
        """

        current_quadkey_extent = utils.quadkey_to_extent(self.current_quadkey)
        # prepare roads
        transportation_sdf = self.input_data_objects[BronzeTransportationDataObject.ID].df.select(
            ColNames.category,
            ColNames.geometry,
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        transportation_sdf = utils.filter_geodata_to_extent(transportation_sdf, current_quadkey_extent, 3035)

        transportation_sdf = self.calculate_transportation_buffer(
            transportation_sdf, self.transportation_category_buffer_m
        )
        transportation_sdf = transportation_sdf.withColumn(ColNames.category, F.lit("roads"))
        # This is needed to reduce number of geometries so all small roads are merged into one multipolygon based on quadkey
        transportation_sdf = self.merge_transportation_by_grid(transportation_sdf, 1000)

        transportation_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        transportation_sdf.count()
        transportation_sdf = transportation_sdf.withColumn("quadkey", F.lit(self.current_quadkey))
        transportation_sdf.coalesce(1).write.mode("overwrite").format("geoparquet").partitionBy("quadkey").save(
            "/opt/mobloc_data/roads_merged_test"
        )

        transportation_sdf = transportation_sdf.select(
            ColNames.category,
            ColNames.geometry,
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        self.logger.info("Roads prepared")

        # prepare landuse
        landuse_sdf = self.input_data_objects[BronzeLanduseDataObject.ID].df.select(
            ColNames.category,
            ColNames.geometry,
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        landuse_sdf = utils.filter_geodata_to_extent(landuse_sdf, current_quadkey_extent, 3035)
        landuse_sdf = utils.cut_geodata_to_extent(landuse_sdf, current_quadkey_extent, 3035)

        landuse_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landuse_sdf.count()
        self.logger.info("Landuse prepared")

        # merge roads with landuse
        landuse_sdf = utils.cut_polygons_with_mask_polygons(
            landuse_sdf,
            transportation_sdf,
            [
                ColNames.category,
                ColNames.geometry,
                ColNames.year,
                ColNames.month,
                ColNames.day,
            ],
            False,
            ColNames.geometry,
        )

        landuse_roads_sdf = landuse_sdf.union(transportation_sdf)

        # blow up too big landuse polygons
        # TODO: make vertices number parameter
        landuse_roads_sdf = landuse_roads_sdf.withColumn(
            ColNames.geometry, STF.ST_SubDivideExplode(ColNames.geometry, 1000)
        )
        landuse_roads_sdf = utils.fix_geometry(landuse_roads_sdf, 3)

        landuse_roads_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        land_use_count = landuse_roads_sdf.count()

        if land_use_count == 0:
            self.logger.warning(f"No data found for quadkey {self.current_quadkey}. Skipping")
            return self.populate_grid_tiles_with_empty_ratios(grid_sdf).drop("geometry")

        self.logger.info("Landuse and roads merged")

        # count landuse types ratios in grid cells
        grid_tiles = self.grid_generator.grid_ids_to_tiles(grid_sdf)
        grid_tiles = grid_tiles.withColumn("area", STF.ST_Area(ColNames.geometry))

        grid_tiles = grid_tiles.persist(StorageLevel.MEMORY_AND_DISK)
        grid_tiles.count()

        grid_tiles = self.calculate_landuse_ratios_per_tile(grid_tiles, landuse_roads_sdf)

        transportation_sdf.unpersist()
        landuse_roads_sdf.unpersist()
        landuse_sdf.unpersist()

        return grid_tiles

    def calculated_landuse_ratios_weighted_sum(
        self,
        grid_tiles: DataFrame,
        weights_dict: Dict[str, float],
        sum_column_name: str,
    ) -> DataFrame:
        """
        Calculates the weighted sum of land use ratios for each grid tile.

        This function takes a DataFrame of grid tiles and a dictionary of weights for each land use ratio.
        It multiplies each land use ratio by its corresponding weight and then sums these weighted ratios
        to calculate a weighted sum for each grid tile.

        Args:
            grid_tiles (pyspark.sql.DataFrame): The DataFrame of grid tiles with landuse ratios.
            weights_dict (dict): A dictionary where the keys are the names of the land use ratios
            and the values are the corresponding weights.
            sum_column_name (str): The name of the new column that will contain the weighted sums.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of grid tiles with the new column of weighted sums.
        """

        weighted_columns = [F.col(f"{ratio}_ratio") * weight for ratio, weight in weights_dict.items()]
        grid_tiles = grid_tiles.withColumn(sum_column_name, sum(weighted_columns))

        return grid_tiles

    def calculate_landuse_ratios_per_tile(self, grid_tiles: DataFrame, landuse_sdf: DataFrame) -> DataFrame:
        """
        Calculates the land use ratios for each tile in a grid.

        This function takes a DataFrame of grid tiles and a DataFrame of land use data.
        For each land use class, it calculates the ratio of the area of the class that intersects
        with each grid tile to the area of the tile.
        If a tile does not intersect with a land use class, the ratio for that class is set to 0.0.
        If a tile does not intersect with any land use class, the 'open_area_ratio' is set to 1.0.

        Args:
            grid_tiles (pyspark.sql.DataFrame): The DataFrame of grid tiles.
            landuse_sdf (pyspark.sql.DataFrame): The DataFrame of land use data.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of grid tiles with the new columns of land use ratios.
        """

        classes = [prior_weights for prior_weights in self.prior_weights.keys()]

        for landuse_class in classes:

            class_area_ratio_sdf = self.find_intersection_ratio(grid_tiles, landuse_sdf, landuse_class)
            if class_area_ratio_sdf.count() == 0:
                grid_tiles = grid_tiles.withColumn(f"{landuse_class}_ratio", F.lit(0.0))
            else:
                grid_tiles = grid_tiles.join(class_area_ratio_sdf, "grid_id", "left")
            grid_tiles = grid_tiles.persist(StorageLevel.MEMORY_AND_DISK)
            grid_tiles.count()
            self.logger.info(f"Landuse class {landuse_class} ratio per tile calculated")

        # Fill null values in the DataFrame with a specific float number, e.g., 0.0
        grid_tiles = grid_tiles.fillna(0.0)

        # Update 'open_areas_ratio' to 1.0 if all other specific ratio columns are 0.0
        # TODO: remove hardcoded values
        grid_tiles = grid_tiles.withColumn(
            "open_area_ratio",
            F.when(
                (F.col("roads_ratio") == 0.0)
                & (F.col("residential_builtup_ratio") == 0.0)
                & (F.col("other_builtup_ratio") == 0.0)
                & (F.col("forest_ratio") == 0.0)
                & (F.col("water_ratio") == 0.0),
                1.0,
            ).otherwise(F.col("open_area_ratio")),
        ).drop("geometry", "area")

        return grid_tiles

    def populate_grid_tiles_with_empty_ratios(self, grid_tiles: DataFrame) -> DataFrame:
        """
        Populates grid tiles with empty land use ratios.

        This function takes a DataFrame of grid tiles and adds a new column for each land use class in weights dict.
        Each new column is initialized with a value of 0.0, representing an empty land use ratio.

        Args:
            grid_tiles (pyspark.sql.DataFrame): The DataFrame of grid tiles.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of grid tiles with the new columns of empty land use ratios.
        """

        classes = [prior_weights for prior_weights in self.prior_weights.keys()]

        for landuse_class in classes:
            grid_tiles = grid_tiles.withColumn(f"{landuse_class}_ratio", F.lit(0.0))

        return grid_tiles

    def calculate_landuse_prior(self, grid_tiles: DataFrame) -> DataFrame:
        """
        Calculates the prior probability of land use for each tile in a grid.

        This function takes a DataFrame of grid tiles, each with a 'weighted_sum' column representing the weighted sum of land use ratios.
        It calculates the total weighted sum across all tiles and then divides the weighted sum of each tile by this
        total to calculate the prior probability of land use for each tile.

        Args:
            grid_tiles (pyspark.sql.DataFrame): The DataFrame of grid tiles.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of grid tiles with the new column of prior probabilities.
        """

        # Compute total weighted sum for normalization
        total_weighted_sum = grid_tiles.select(F.sum("weighted_sum").alias("total_weighted_sum")).collect()[0][
            "total_weighted_sum"
        ]

        # Normalize the weighted sum across all grid tiles
        grid_tiles = grid_tiles.withColumn(ColNames.prior_probability, F.col("weighted_sum") / total_weighted_sum)

        return grid_tiles

    def calculate_transportation_buffer(
        self, transportation_sdf: DataFrame, category_buffer_m: Dict[str, float]
    ) -> DataFrame:
        """
        Calculates the buffer for each transportation feature based on its category.

        This function takes a DataFrame of transportation features and a dictionary mapping categories to buffer distances.
        It assigns the appropriate buffer distance to each feature based on its category and then calculates the buffer geometry.
        The buffer geometry replaces the original geometry of each feature.

        Args:
            transportation_sdf (pyspark.sql.DataFrame): The DataFrame of transportation features.
            category_buffer_m (dict): A dictionary mapping categories to buffer distances.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of transportation features with the buffer geometries.
        """

        transportation_sdf = self.assign_mapping_values(
            transportation_sdf,
            category_buffer_m,
            ColNames.category,
            "buffer_dist",
            category_buffer_m["unknown"],
        )

        transportation_sdf = transportation_sdf.filter(F.col("buffer_dist") != 0.0)

        transportation_sdf = transportation_sdf.withColumn(
            ColNames.geometry,
            STF.ST_Buffer(ColNames.geometry, F.col("buffer_dist")),
        ).drop("buffer_dist")

        return transportation_sdf

    def merge_transportation_by_grid(self, transportation_sdf: DataFrame, resolution: int) -> DataFrame:
        """
        Merges transportation data with a generated grid based on the specified resolution.

        This function takes a DataFrame containing transportation data and a grid resolution.
        It first generates a grid that covers the extent of the transportation data. Then, it
        intersects the transportation data with this grid, merging the transportation geometries
        that fall within each grid cell. The result is a DataFrame where each row represents a
        grid cell, aggregated by transportation category and date, with a merged geometry for
        all transportation data within that cell.

        Parameters:
        - transportation_sdf (DataFrame): A Spark DataFrame containing transportation data.
        - resolution (int): The resolution of the grid to generate, specified as the length
        of the side of each square grid cell in meters.

        Returns:
        - DataFrame: A Spark DataFrame where each row represents merged transportation data.
        """
        grid_gen = InspireGridGenerator(self.spark, resolution)
        for_extent = transportation_sdf.groupBy().agg(STA.ST_Envelope_Aggr("geometry").alias("envelope"))
        for_extent = for_extent.withColumn(
            "envelope",
            STF.ST_Transform("envelope", F.lit("epsg:3035"), F.lit("epsg:4326")),
        )
        extent = (
            for_extent.withColumn(
                "envelope",
                F.array(
                    STF.ST_XMin("envelope"),
                    STF.ST_YMin("envelope"),
                    STF.ST_XMax("envelope"),
                    STF.ST_YMax("envelope"),
                ),
            )
            .collect()[0]
            .envelope
        )
        for_extent = grid_gen.cover_extent_with_grid_tiles(extent)

        intersection = (
            transportation_sdf.alias("a")
            .join(
                for_extent.alias("b"),
                STP.ST_Intersects("a.geometry", "b.geometry"),
            )
            .withColumn("merge_geometry", STF.ST_Intersection("a.geometry", "b.geometry"))
            .groupBy("category", "year", "month", "day", "b.grid_id")
            .agg(STA.ST_Union_Aggr("merge_geometry").alias("geometry"))
        )

        return intersection

    def find_intersection_ratio(self, grid_tiles: DataFrame, landuse: DataFrame, landuse_class: str) -> DataFrame:
        """
        Finds the intersection ratio of a specific land use class for each tile in a grid.

        This function takes a DataFrame of grid tiles, a DataFrame of land use data, and a land use class.
        It finds the intersection of each grid tile with the land use data for the specified class,
        calculates the area of the intersection, and divides this by the area of the tile to find the intersection ratio.
        The intersection ratio is added as a new column to the grid DataFrame.

        Args:
            grid_tiles (pyspark.sql.DataFrame): The DataFrame of grid tiles.
            landuse (pyspark.sql.DataFrame): The DataFrame of land use data.
            landuse_class (str): The land use class to find the intersection ratio for.

        Returns:
            pyspark.sql.DataFrame: The DataFrame of grid tiles with the new column of intersection ratios.
        """

        grid_tiles = grid_tiles.alias("a").join(
            landuse.filter(F.col(ColNames.category) == F.lit(landuse_class)).alias("b"),
            STP.ST_Intersects(f"a.{ColNames.geometry}", f"b.{ColNames.geometry}"),
        )

        grid_tiles = grid_tiles.withColumn(
            "shared_geom",
            STF.ST_Intersection(f"a.{ColNames.geometry}", f"b.{ColNames.geometry}"),
        ).select(f"b.{ColNames.category}", f"a.{ColNames.grid_id}", "a.area", "shared_geom")

        # grid_tiles = utils.fix_polygon_geometry(grid_tiles, "shared_geom")

        grid_tiles = grid_tiles.groupBy(f"b.{ColNames.category}", f"a.{ColNames.grid_id}", "a.area").agg(
            F.sum(STF.ST_Area("shared_geom")).alias(f"{landuse_class}_area")
        )

        grid_tiles = grid_tiles.withColumn(f"{landuse_class}_ratio", F.col(f"{landuse_class}_area") / F.col("area"))

        return grid_tiles.select(ColNames.grid_id, f"{landuse_class}_ratio")

    @staticmethod
    def assign_mapping_values(
        sdf: DataFrame,
        values_map: Dict[Any, Any],
        map_column: str,
        values_column: str,
        default_value: Any = 2,
    ) -> DataFrame:
        """
        Assigns mapping values to a DataFrame based on a specified column.

        This function takes a DataFrame, a dictionary of mapping values, a column to map from,
        a column to map to, and a default value. It creates a new column in the DataFrame by mapping values
        from the map column to the values column using the values map.
        If a value in the map column is not found in the values map, the default value is used.

        Args:
            sdf (pyspark.sql.DataFrame): The DataFrame to assign mapping values to.
            values_map (dict): The dictionary of mapping values.
            map_column (str): The column to map from.
            values_column (str): The column to map to.
            default_value (any, optional): The default value to use if a value in the map column is not found in the values map. Defaults to 2.

        Returns:
            pyspark.sql.DataFrame: The DataFrame with the new column of mapped values.
        """

        keys = list(values_map.keys())
        reclass_expr = F.when(F.col(map_column) == (keys[0]), values_map[keys[0]])

        for key in keys[1:]:
            reclass_expr = reclass_expr.when(F.col(map_column) == (key), values_map[key])

        reclass_expr = reclass_expr.otherwise(default_value)
        sdf = sdf.withColumn(values_column, reclass_expr)

        return sdf
