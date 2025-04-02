"""
This module is responsible for enrichment of the operational grid with elevation and landuse data.
"""

from functools import reduce
import operator
from typing import Any, Dict

from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

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
import multimno.core.quadkey_utils as quadkey_utils


class GridEnrichment(Component):
    """
    This class is responsible for enrichment of the operational grid with elevation and landuse data.
    """

    COMPONENT_ID = "GridEnrichment"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.do_landuse_enrichment = self.config.getboolean(self.COMPONENT_ID, "do_landuse_enrichment")
        self.transportation_category_buffer_m = self.config.geteval(
            self.COMPONENT_ID, "transportation_category_buffer_m"
        )

        self.do_elevation_enrichment = self.config.getboolean(self.COMPONENT_ID, "do_elevation_enrichment")

        self.quadkey_batch_size = self.config.getint(self.COMPONENT_ID, "quadkey_batch_size")

        # TODO: For now set to default 100, but can be dynamic from config in future
        self.grid_resolution = 100

        self.grid_generator = InspireGridGenerator(self.spark)

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
            self.spark, grid_do_path
        )

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        # get quadkeys to process to list
        self.quadkeys_to_process = sorted(
            self.input_data_objects[SilverGridDataObject.ID]
            .df.select("quadkey")
            .distinct()
            .rdd.map(lambda x: x.quadkey)
            .collect()
        )
        self.logger.info("Quadkeys to process: ")
        self.logger.info(f"{self.quadkeys_to_process}")
        # generate quadkey batches
        self.quadkey_batches = self.generate_batches(self.quadkeys_to_process, self.quadkey_batch_size)
        self.logger.info(f"Will be processrd in: {len(self.quadkey_batches)} parts")
        processed = 0
        self.origin = self.input_data_objects[SilverGridDataObject.ID].df.select(ColNames.origin).first().origin
        for quadkey_batch in self.quadkey_batches:
            self.logger.info(f"Processing quadkeys {quadkey_batch}")
            self.current_quadkey_batch = quadkey_batch
            self.transform()
            self.write()
            self.spark.catalog.clearCache()
            processed += 1
            self.logger.info(f"Processed {processed} out of {len(self.quadkey_batches)} batches")
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def generate_batches(self, elements_list, batch_size):
        """
        Generates batches of elements from list.
        """
        return [elements_list[i : i + batch_size] for i in range(0, len(elements_list), batch_size)]

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        current_grid_part_sdf = self.input_data_objects[SilverGridDataObject.ID].df.filter(
            F.col(ColNames.quadkey).isin(self.current_quadkey_batch)
        )

        current_grid_part_sdf = current_grid_part_sdf.withColumn(ColNames.elevation, F.lit(None))
        if self.do_elevation_enrichment:
            current_grid_part_sdf = self.add_elevation_to_grid()

        if self.do_landuse_enrichment:

            # preapre transportation data
            transportation_sdf = self.prepare_transportation_data()
            transportation_sdf.persist()
            transportation_count = transportation_sdf.count()
            self.logger.info(f"Transportation data prepared...")
            # # prepare landuse
            landuse_sdf = self.input_data_objects[BronzeLanduseDataObject.ID].df.filter(
                F.col(ColNames.quadkey).isin(self.current_quadkey_batch)
            )

            # merge landuse and transportation
            if transportation_count > 0:
                landuse_sdf = utils.clip_polygons_with_mask_polygons(
                    landuse_sdf,
                    transportation_sdf,
                    [
                        ColNames.category,
                        ColNames.geometry,
                        ColNames.quadkey,
                    ],
                    False,
                    ColNames.geometry,
                )

            landuse_roads_sdf = landuse_sdf.union(transportation_sdf)
            landuse_roads_sdf.persist()
            land_use_count = landuse_roads_sdf.count()
            self.logger.info(f"Landuse data prepared...")

            if land_use_count == 0:
                self.logger.warning(
                    f"No landuse data found for quadkey {self.current_quadkey_batch}. Assigning default open_area"
                )
                current_grid_part_sdf = current_grid_part_sdf.withColumn(
                    ColNames.main_landuse_category, F.lit("open_area")
                ).withColumn(
                    ColNames.landuse_area_ratios,
                    F.create_map(F.lit("open_area"), F.lit(1.0)),
                )
            else:
                # count landuse types ratios in grid cells
                current_grid_part_sdf = self.grid_generator.grid_ids_to_grid_tiles(
                    current_grid_part_sdf, 100, origin=self.origin
                )
                current_grid_part_sdf = current_grid_part_sdf.withColumn("area", STF.ST_Area(ColNames.geometry))

                landuse_roads_sdf = landuse_roads_sdf.withColumn(
                    ColNames.geometry, STF.ST_SubDivideExplode(ColNames.geometry, 1000)
                )

                tiles_with_landuse_sdf = self.calculate_grid_landuse_area_ratios(
                    current_grid_part_sdf, landuse_roads_sdf
                )

                # get all other tiles that do not have landuse data
                all_tiles_sdf = current_grid_part_sdf.join(
                    tiles_with_landuse_sdf,
                    on=[ColNames.grid_id, ColNames.quadkey],
                    how="left",
                )

                # and assign default open_area
                all_tiles_sdf = all_tiles_sdf.withColumn(
                    ColNames.category, F.coalesce(F.col(ColNames.category), F.lit("open_area"))
                ).withColumn("area_ratio", F.coalesce(F.col("area_ratio"), F.lit(1.0)))

                # get main landuse category and landuse area ratios for each grid tile
                current_grid_part_sdf = all_tiles_sdf.groupBy(
                    ColNames.grid_id, ColNames.elevation, ColNames.quadkey
                ).agg(
                    F.map_from_entries(F.collect_list(F.struct(ColNames.category, "area_ratio"))).alias(
                        ColNames.landuse_area_ratios
                    ),
                    F.max_by(
                        F.when(F.col("area_ratio") > 0.05, F.col("category")).otherwise(None), F.col("area_ratio")
                    ).alias(ColNames.main_landuse_category),
                )

                # Then also assign mauin landuse category to open_area if it is not assigned
                current_grid_part_sdf = current_grid_part_sdf.withColumn(
                    ColNames.main_landuse_category,
                    F.coalesce(F.col(ColNames.main_landuse_category), F.lit("open_area")),
                )

            current_grid_part_sdf = self.grid_generator.grid_ids_to_grid_centroids(
                current_grid_part_sdf, 100, origin=self.origin
            )

        current_grid_part_sdf = utils.apply_schema_casting(current_grid_part_sdf, SilverEnrichedGridDataObject.SCHEMA)
        current_grid_part_sdf = current_grid_part_sdf.repartition(*SilverEnrichedGridDataObject.PARTITION_COLUMNS)
        self.output_data_objects[SilverEnrichedGridDataObject.ID].df = current_grid_part_sdf

    def add_elevation_to_grid(self):
        # TODO: implement elevation enrichment
        pass

    def prepare_transportation_data(self) -> DataFrame:
        """
        Prepares transportation data for mapping to a grid.

        This function takes the transportation data DataFrame, filters it to the extent of the current quadkey,
        and then calculates a buffer around each transportation feature based on its category.

        Returns:
            pyspark.sql.DataFrame: The prepared transportation data.
        """

        transportation_sdf = self.input_data_objects[BronzeTransportationDataObject.ID].df
        transportation_sdf = transportation_sdf.filter(F.col(ColNames.quadkey).isin(self.current_quadkey_batch))

        if transportation_sdf.count() == 0:
            self.logger.warning(f"No transportation data found for quadkey {self.current_quadkey_batch}. Skipping")

            return self.spark.createDataFrame([], transportation_sdf.schema)

        transportation_sdf = self.calculate_transportation_buffer(
            transportation_sdf, self.transportation_category_buffer_m
        )

        # prepare quadkeys geometries to merge roads
        child_quadkeys = [quadkey_utils.get_children_quadkeys(quadkey, 16) for quadkey in self.current_quadkey_batch]
        child_quadkeys = reduce(operator.add, child_quadkeys, [])

        quadkeys_mask_sdf = quadkey_utils.quadkeys_to_extent_dataframe(self.spark, child_quadkeys, 3035)
        quadkeys_mask_sdf = quadkeys_mask_sdf.withColumnRenamed(ColNames.quadkey, "mask_quadkey")

        transportation_sdf = utils.merge_geom_within_mask_geom(
            transportation_sdf, quadkeys_mask_sdf, [ColNames.quadkey, "mask_quadkey"], ColNames.geometry
        )

        return transportation_sdf.select(F.lit("roads").alias(ColNames.category), ColNames.geometry, ColNames.quadkey)

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

    def calculate_grid_landuse_area_ratios(self, grid_tiles: DataFrame, landuse: DataFrame) -> DataFrame:
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

        grid_tiles = grid_tiles.select(F.col(ColNames.geometry).alias("grid_geometry"), ColNames.grid_id, "area").join(
            landuse,
            STP.ST_Intersects("grid_geometry", f"{ColNames.geometry}"),
        )

        grid_tiles = grid_tiles.withColumn(
            "shared_geom",
            STF.ST_Intersection("grid_geometry", f"{ColNames.geometry}"),
        ).select(ColNames.category, ColNames.grid_id, ColNames.quadkey, "area", "shared_geom")

        grid_tiles = grid_tiles.groupBy(ColNames.grid_id, ColNames.category, ColNames.quadkey, "area").agg(
            F.sum(STF.ST_Area("shared_geom")).alias(f"category_area")
        )

        grid_tiles = grid_tiles.withColumn("area_ratio", F.col(f"category_area") / F.col("area"))

        # Define window spec for sum calculation per grid cell
        window_spec = Window.partitionBy(ColNames.grid_id, ColNames.quadkey)

        # Calculate total ratio and normalize areas if total ratio is greater than 1
        grid_tiles = (
            grid_tiles.withColumn("total_ratio", F.sum("area_ratio").over(window_spec))
            .withColumn(
                "area_ratio",
                F.when(F.col("total_ratio") > 1.0, F.col("area_ratio") / F.col("total_ratio")).otherwise(
                    F.col("area_ratio")
                ),
            )
            .drop("total_ratio", "category_area", "area")
        )  # Drop unnecessary columns

        return grid_tiles.select(ColNames.grid_id, ColNames.category, ColNames.quadkey, "area_ratio")

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
