"""
This module contains the InspireGridGeneration class which is responsible for generating the INSPIRE grid 
and enrich it with elevation and landuse data.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from sedona.sql import st_functions as STF
from sedona.sql import st_aggregates as STA

from multimno.core.component import Component

from multimno.core.data_objects.bronze.bronze_countries_data_object import (
    BronzeCountriesDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.grid import InspireGridGenerator
from multimno.core.spark_session import delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY, CONFIG_BRONZE_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils


class InspireGridGeneration(Component):
    """
    This class is responsible for generating the INSPIRE grid for given extent or polygon
    and enrich it with elevation and landuse data.
    """

    COMPONENT_ID = "InspireGridGeneration"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.grid_extent = self.config.geteval(InspireGridGeneration.COMPONENT_ID, "extent")
        self.grid_partition_size = self.config.get(InspireGridGeneration.COMPONENT_ID, "grid_generation_partition_size")

        self.grid_quadkey_level = self.config.getint(
            InspireGridGeneration.COMPONENT_ID,
            "grid_processing_partition_quadkey_level",
        )

        self.reference_country = self.config.get(InspireGridGeneration.COMPONENT_ID, "reference_country")

        self.country_buffer = self.config.get(InspireGridGeneration.COMPONENT_ID, "country_buffer")

        # TODO: For now set to default 100, but can be dynamic from config in future
        self.grid_resolution = 100

        self.grid_generator = InspireGridGenerator(
            self.spark,
            self.grid_resolution,
            ColNames.geometry,
            ColNames.grid_id,
            self.grid_partition_size,
        )

        self.quadkey_udf = F.udf(utils.latlon_to_quadkey)

    def initalize_data_objects(self):

        self.clear_destination_directory = self.config.getboolean(
            InspireGridGeneration.COMPONENT_ID, "clear_destination_directory"
        )

        self.grid_mask = self.config.get(InspireGridGeneration.COMPONENT_ID, "grid_mask")

        if self.grid_mask == "polygon":
            # inputs
            self.input_data_objects = {}
            self.input_data_objects[BronzeCountriesDataObject.ID] = BronzeCountriesDataObject(
                self.spark,
                self.config.get(CONFIG_BRONZE_PATHS_KEY, "countries_data_bronze"),
            )

        # outputs

        grid_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, grid_do_path)

        self.output_data_objects = {}
        self.output_data_objects[SilverGridDataObject.ID] = SilverGridDataObject(
            self.spark, grid_do_path, [ColNames.quadkey]
        )

    def execute(self):

        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        if self.grid_mask == "polygon":
            self.read()

            countries = self.get_country_mask(self.reference_country, self.country_buffer)
            ids = [row["temp_id"] for row in countries.select("temp_id").collect()]

            self.logger.info(f"Processing {len(ids)} parts of the country")
            processed_parts = 0
            for id in ids:
                self.current_country_part = countries.filter(F.col("temp_id") == id)
                self.transform()
                self.write()
                processed_parts += 1
                self.logger.info(f"Finished processing {processed_parts} parts")
        else:
            self.transform()
            self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        if self.grid_mask == "polygon":
            grid_sdf = self.grid_generator.cover_polygon_with_grid_centroids(self.current_country_part)
        else:
            grid_sdf = self.grid_generator.cover_extent_with_grid_centroids(self.grid_extent)
        grid_sdf = utils.assign_quadkey(grid_sdf, 3035, self.grid_quadkey_level)

        grid_sdf = grid_sdf.orderBy(ColNames.quadkey)
        grid_sdf = grid_sdf.repartition(ColNames.quadkey)

        grid_sdf = utils.apply_schema_casting(grid_sdf, SilverGridDataObject.SCHEMA)

        self.output_data_objects[SilverGridDataObject.ID].df = grid_sdf

    def get_country_mask(self, reference_country, country_buffer):

        countries = self.input_data_objects[BronzeCountriesDataObject.ID].df
        countries = (
            countries.filter(F.col(ColNames.iso2) == reference_country)
            .withColumn(
                ColNames.geometry,
                STF.ST_Buffer(ColNames.geometry, F.lit(country_buffer)),
            )
            .groupBy()
            .agg(STA.ST_Union_Aggr(ColNames.geometry).alias(ColNames.geometry))
            .withColumn(ColNames.geometry, F.explode(STF.ST_Dump(ColNames.geometry)))
            .withColumn("temp_id", F.monotonically_increasing_id())
        )

        countries = utils.project_to_crs(countries, 3035, 4326)

        return countries
