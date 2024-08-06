"""

"""

import math
from typing import Dict, List, Optional, Tuple

from sedona.sql import st_functions as STF

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.core.spark_session import delete_file_or_folder
from multimno.core.component import Component
from multimno.core.data_objects.landing.landing_http_geojson_data_object import (
    LandingHttpGeoJsonDataObject,
)
from multimno.core.data_objects.bronze.bronze_countries_data_object import (
    BronzeCountriesDataObject,
)
from multimno.core.data_objects.bronze.bronze_geographic_zones_data_object import (
    BronzeGeographicZonesDataObject,
)

from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils


class GiscoDataIngestion(Component):
    """ """

    COMPONENT_ID = "GiscoDataIngestion"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

    def initalize_data_objects(self):

        base_url = self.config.get(GiscoDataIngestion.COMPONENT_ID, "base_url")

        self.get_countries = self.config.getboolean(GiscoDataIngestion.COMPONENT_ID, "get_countries")

        self.get_nuts = self.config.getboolean(GiscoDataIngestion.COMPONENT_ID, "get_nuts")

        self.default_crs = self.config.getint(GiscoDataIngestion.COMPONENT_ID, "default_crs")

        self.input_data_objects = {}
        self.output_data_objects = {}
        if self.get_countries:

            countries_resolution = self.config.get(GiscoDataIngestion.COMPONENT_ID, "countries_resolution")
            countries_year = self.config.get(GiscoDataIngestion.COMPONENT_ID, "countries_year")

            countries_url = (
                f"{base_url}/countries/geojson/CNTR_RG_{countries_resolution}M_{countries_year}_4326.geojson"
            )

            self.input_data_objects["countries"] = LandingHttpGeoJsonDataObject(self.spark, countries_url, 240, 3)

            countries_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "countries_data_bronze")

            self.output_data_objects[BronzeCountriesDataObject.ID] = BronzeCountriesDataObject(
                self.spark,
                countries_do_path,
                [],
                self.default_crs,
            )

        if self.get_nuts:

            nuts_resolution = self.config.get(GiscoDataIngestion.COMPONENT_ID, "nuts_resolution")
            self.nuts_year = self.config.get(GiscoDataIngestion.COMPONENT_ID, "nuts_year")
            nuts_levels = self.config.get(GiscoDataIngestion.COMPONENT_ID, "nuts_levels")
            self.nuts_levels = nuts_levels.split(",")

            self.reference_country = self.config.get(GiscoDataIngestion.COMPONENT_ID, "reference_country")
            for level in self.nuts_levels:
                nuts_url = (
                    f"{base_url}/nuts/geojson/NUTS_RG_{nuts_resolution}M_{self.nuts_year}_4326_LEVL_{level}.geojson"
                )
                self.input_data_objects[f"nuts_{level}"] = LandingHttpGeoJsonDataObject(self.spark, nuts_url, 300, 5)

            geographic_zones_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "geographic_zones_data_bronze")

            self.output_data_objects[BronzeGeographicZonesDataObject.ID] = BronzeGeographicZonesDataObject(
                self.spark,
                geographic_zones_do_path,
                [ColNames.dataset_id],
                self.default_crs,
            )

        self.clear_destination_directory = self.config.getboolean(
            GiscoDataIngestion.COMPONENT_ID, "clear_destination_directory"
        )

        if self.clear_destination_directory:
            for do in self.output_data_objects.values():
                self.logger.info(f"Clearing {do.default_path}")
                delete_file_or_folder(self.spark, do.default_path)

    def read(self):
        # need to read the data from the input data objects separately
        pass

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        if self.get_countries:

            countries = self.get_countries_data()
            countries = utils.apply_schema_casting(countries, BronzeCountriesDataObject.SCHEMA)
            self.output_data_objects[BronzeCountriesDataObject.ID].df = countries

        if self.get_nuts:
            self.output_data_objects[BronzeGeographicZonesDataObject.ID].df = self.spark.createDataFrame(
                [], BronzeGeographicZonesDataObject.SCHEMA
            )
            for level in self.nuts_levels:

                nuts = self.get_nuts_data(level)
                nuts = nuts.withColumn(ColNames.dataset_id, F.lit("nuts"))
                nuts = utils.apply_schema_casting(nuts, BronzeGeographicZonesDataObject.SCHEMA)
                self.output_data_objects[BronzeGeographicZonesDataObject.ID].df = self.output_data_objects[
                    BronzeGeographicZonesDataObject.ID
                ].df.union(nuts)

    def get_countries_data(self) -> DataFrame:
        """
        Retrieves and processes country data.

        This method reads country data from the GISCO portal
        and processes the data by renaming columns, exploding the 'geometry' column,
        and projecting the data to a default CRS.
        The processed data is returned as a DataFrame.

        Returns:
            DataFrame: A DataFrame containing processed country data.
                    The DataFrame has columns for the ISO 2 country code, country name, and geometry.
        """

        self.input_data_objects["countries"].read()
        self.logger.info(f"got countries data")
        countries_sdf = self.input_data_objects["countries"].df
        countries_sdf = countries_sdf.withColumnRenamed("CNTR_ID", ColNames.iso2).withColumnRenamed(
            "NAME_ENGL", ColNames.name
        )

        countries_sdf = countries_sdf.withColumn("geometry", F.explode(STF.ST_Dump("geometry")))

        countries_sdf = utils.project_to_crs(countries_sdf, 4326, self.default_crs)
        return countries_sdf

    def get_nuts_data(self, level: str) -> DataFrame:
        """
        Retrieves and processes NUTS (Nomenclature of Territorial Units for Statistics) data for a specific level.

        This method reads NUTS data from GISCO portal and processes the data by
        renaming columns, filtering by reference country, and projecting the data to a default CRS.
        It also adds a 'parent_id' column that contains the ID of the parent NUTS unit for each NUTS unit,
        and adds columns for the year, month, and day with fixed values.
        The processed data is returned as a DataFrame.

        Args:
            level (str): The NUTS level for which to retrieve and process data.

        Returns:
            DataFrame: A DataFrame containing processed NUTS data.
                    The DataFrame has columns for the NUTS ID, name, ISO 2 country code, level,
                    geometry, parent ID, year, month, and day.
        """

        self.input_data_objects[f"nuts_{level}"].read()
        self.logger.info(f"got NUTS data for level {level}")
        nuts_sdf = self.input_data_objects[f"nuts_{level}"].df
        nuts_sdf = nuts_sdf.drop("id")
        nuts_sdf = nuts_sdf.filter(F.col("CNTR_CODE") == self.reference_country)

        nuts_sdf = (
            nuts_sdf.withColumnRenamed("NUTS_ID", ColNames.zone_id)
            .withColumnRenamed("NAME_LATN", ColNames.name)
            .withColumnRenamed("CNTR_CODE", ColNames.iso2)
            .withColumnRenamed("LEVL_CODE", ColNames.level)
        )

        nuts_sdf = nuts_sdf.select(
            ColNames.zone_id,
            ColNames.name,
            ColNames.iso2,
            ColNames.level,
            ColNames.geometry,
        )

        if level == 0:
            nuts_sdf = nuts_sdf.withColumn("parent_id", F.lit(None))
        else:
            nuts_sdf = nuts_sdf.withColumn("parent_id", F.expr("substring(zone_id, 1, length(zone_id)-1)"))

        nuts_sdf = utils.project_to_crs(nuts_sdf, 4326, self.default_crs)

        nuts_sdf = nuts_sdf.withColumns(
            {
                ColNames.year: F.lit(self.nuts_year),
                ColNames.month: F.lit(1),
                ColNames.day: F.lit(1),
            }
        )
        return nuts_sdf
