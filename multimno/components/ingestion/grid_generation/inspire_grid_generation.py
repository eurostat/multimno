"""
This module contains the InspireGridGeneration class which is responsible for generating the INSPIRE grid 
and enrich it with elevation and landuse data.
"""

import pyspark.sql.functions as F

from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from multimno.core.grid import InspireGridGenerator

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames


class InspireGridGeneration(Component):
    """
    This class is responsible for generating the INSPIRE grid for given extent or polygon
    and enrich it with elevation and landuse data.
    """

    COMPONENT_ID = "InspireGridGeneration"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.do_elevation_enrichment = self.config.get(InspireGridGeneration.COMPONENT_ID, "do_elevation_enrichment")
        self.do_land_cover_enrichment = self.config.get(InspireGridGeneration.COMPONENT_ID, "do_landcover_enrichment")

        exent_dict = self.config.geteval(InspireGridGeneration.COMPONENT_ID, "extent")

        self.grid_extent = [exent_dict["min_lon"], exent_dict["min_lat"], exent_dict["max_lon"], exent_dict["max_lat"]]
        self.grid_partition_size = self.config.get(InspireGridGeneration.COMPONENT_ID, "grid_partition_size")

        # TODO: For now set to default 100, but can be dynamic from config in future
        self.grid_resolution = 100

    def initalize_data_objects(self):

        grid_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver")

        self.output_data_objects = {}
        self.output_data_objects[SilverGridDataObject.ID] = SilverGridDataObject(self.spark, grid_do_path, [])

    def read(self):
        # TODO implement read method to read elevation and landuse rasters
        pass

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        grid_generator = InspireGridGenerator(
            self.spark, self.grid_resolution, ColNames.geometry, ColNames.grid_id, self.grid_partition_size
        )

        grid_sdf = grid_generator.cover_extent_with_grid_centroids(self.grid_extent)

        if self.do_elevation_enrichment:
            self.add_elevation_to_grid()

        if self.do_land_cover_enrichment:
            self.add_landcover_to_grid()

        # Cast column types to DO schema, add missing columns manually
        df_columns = set(grid_sdf.columns)
        schema_columns = set(field.name for field in SilverGridDataObject.SCHEMA.fields)
        missing_columns = schema_columns - df_columns

        for column in missing_columns:
            grid_sdf = grid_sdf.withColumn(column, F.lit(None).cast(SilverGridDataObject.SCHEMA[column].dataType))

        # Apply schema casting
        for field in SilverGridDataObject.SCHEMA.fields:
            grid_sdf = grid_sdf.withColumn(field.name, F.col(field.name).cast(field.dataType))

        # TODO: figure out why createDataFrame from rdd is not working for geometry column and how to make it work

        self.output_data_objects[SilverGridDataObject.ID].df = grid_sdf

    def add_elevation_to_grid(self):
        # TODO: implement elevation enrichment
        pass

    def add_landcover_to_grid(self):
        # TODO: implement landcover enrichment
        pass
