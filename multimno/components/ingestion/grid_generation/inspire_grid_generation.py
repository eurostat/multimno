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
import multimno.core.quadkey_utils as quadkey_utils
from multimno.core.log import get_execution_stats


class InspireGridGeneration(Component):
    """
    This class is responsible for generating the INSPIRE grid for given extent or polygon
    and enrich it with elevation and landuse data.
    """

    COMPONENT_ID = "InspireGridGeneration"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.grid_partition_size = self.config.get(InspireGridGeneration.COMPONENT_ID, "grid_generation_partition_size")

        self.grid_quadkey_level = self.config.getint(
            InspireGridGeneration.COMPONENT_ID,
            "grid_processing_partition_quadkey_level",
        )

        self.reference_country = self.config.get(InspireGridGeneration.COMPONENT_ID, "reference_country")

        self.country_buffer = self.config.get(InspireGridGeneration.COMPONENT_ID, "country_buffer")

        self.grid_generator = InspireGridGenerator(
            self.spark,
            ColNames.geometry,
            ColNames.grid_id,
            self.grid_partition_size,
        )

        # Attributes that will hold the shared common origin if creating INSPIRE grid using polygon(s) instead of
        # an extent
        self.n_origin: float = None
        self.e_origin: float = None

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

    @get_execution_stats
    def execute(self):

        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        if self.grid_mask == "polygon":
            self.read()

            countries, country_extent = self.get_country_mask(self.reference_country, self.country_buffer)
            proj_extent, _ = self.grid_generator.process_latlon_extent(country_extent)
            self.n_origin = proj_extent[0]
            self.e_origin = proj_extent[1]

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
            grid_sdf = self.grid_generator.cover_polygon_with_grid_centroids(
                self.current_country_part, n_origin=self.n_origin, e_origin=self.e_origin
            )
        else:
            grid_extent = self.config.geteval(InspireGridGeneration.COMPONENT_ID, "extent")
            grid_sdf = self.grid_generator.cover_extent_with_grid_centroids(grid_extent)
        grid_sdf = quadkey_utils.assign_quadkey(grid_sdf, 3035, self.grid_quadkey_level)

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
        )

        countries = utils.project_to_crs(countries, 3035, 4326)

        country_extent = countries.select(
            STF.ST_XMin("geometry").alias("longitude_min"),
            STF.ST_YMin("geometry").alias("latitude_min"),
            STF.ST_XMax("geometry").alias("longitude_max"),
            STF.ST_YMax("geometry").alias("latitude_max"),
        ).collect()

        if len(country_extent) == 0:
            raise ValueError(f"BronzeCountriesDataObject for {reference_country} seems to have no geometries")
        country_extent = list(country_extent[0][:])

        countries = countries.withColumn(ColNames.geometry, F.explode(STF.ST_Dump(ColNames.geometry))).withColumn(
            "temp_id", F.monotonically_increasing_id()
        )

        return countries, country_extent
