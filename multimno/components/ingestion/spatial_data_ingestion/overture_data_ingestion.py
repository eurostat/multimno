""" """

from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.core.spark_session import delete_file_or_folder
from multimno.core.component import Component
from multimno.core.data_objects.landing.landing_geoparquet_data_object import (
    LandingGeoParquetDataObject,
)

from multimno.core.settings import CONFIG_LANDING_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
import multimno.core.quadkey_utils as quadkey_utils


class OvertureDataIngestion(Component):
    """ """

    COMPONENT_ID = "OvertureDataIngestion"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.extent = self.config.geteval(self.COMPONENT_ID, "extent")
        self.extraction_quadkey_level = self.config.getint(self.COMPONENT_ID, "extraction_quadkey_level")
        self.quadkeys_to_process = self.config.geteval(self.COMPONENT_ID, "quadkeys_to_process")

    def initalize_data_objects(self):

        self.transportation = "transportation"
        self.landuse = "landuse"
        self.landcover = "landcover"
        self.buildings = "buildings"
        self.water = "water"

        overture_url = self.config.get(self.COMPONENT_ID, "overture_url")

        transportation_url = overture_url + "/theme=transportation/type=segment"
        buildings_url = overture_url + "/theme=buildings/type=building"
        landcover_url = overture_url + "/theme=base/type=land"
        landuse_url = overture_url + "/theme=base/type=land_use"
        water_url = overture_url + "/theme=base/type=water"

        self.input_data_objects = {}
        self.input_data_objects[self.transportation] = LandingGeoParquetDataObject(
            self.spark, transportation_url, set_crs=False
        )
        self.input_data_objects[self.buildings] = LandingGeoParquetDataObject(self.spark, buildings_url, set_crs=False)
        self.input_data_objects[self.landcover] = LandingGeoParquetDataObject(self.spark, landcover_url, set_crs=False)
        self.input_data_objects[self.landuse] = LandingGeoParquetDataObject(self.spark, landuse_url, set_crs=False)
        self.input_data_objects[self.water] = LandingGeoParquetDataObject(self.spark, water_url, set_crs=False)

        self.clear_destination_directory = self.config.getboolean(
            OvertureDataIngestion.COMPONENT_ID, "clear_destination_directory"
        )

        transportation_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "transportation_data_landing")
        landuse_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "landuse_data_landing")
        landcover_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "landcover_data_landing")
        buildings_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "buildings_data_landing")
        water_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "water_data_landing")

        self.output_data_objects = {}

        self.output_data_objects[self.transportation] = LandingGeoParquetDataObject(
            self.spark, transportation_do_path, [ColNames.quadkey]
        )
        self.output_data_objects[self.landuse] = LandingGeoParquetDataObject(
            self.spark, landuse_do_path, [ColNames.quadkey]
        )
        self.output_data_objects[self.landcover] = LandingGeoParquetDataObject(
            self.spark, landcover_do_path, [ColNames.quadkey]
        )
        self.output_data_objects[self.buildings] = LandingGeoParquetDataObject(
            self.spark, buildings_do_path, [ColNames.quadkey]
        )
        self.output_data_objects[self.water] = LandingGeoParquetDataObject(
            self.spark, water_do_path, [ColNames.quadkey]
        )

        if self.clear_destination_directory:
            for do in self.output_data_objects.values():
                self.logger.info(f"Clearing {do.default_path}")
                delete_file_or_folder(self.spark, do.default_path)

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        if len(self.quadkeys_to_process) == 0:
            self.quadkeys_to_process = quadkey_utils.get_quadkeys_for_bbox(self.extent, self.extraction_quadkey_level)
        self.logger.info(f"Extraction will be done in {len(self.quadkeys_to_process)} parts.")

        self.logger.info("quadkeys to process: ")
        self.logger.info(f"{self.quadkeys_to_process}")
        processed = 0
        for quadkey in self.quadkeys_to_process:
            self.logger.info(f"Processing quadkey {quadkey}")
            self.current_extent = quadkey_utils.quadkey_to_extent(quadkey)
            self.current_quadkey = quadkey
            self.transform()
            processed += 1
            self.logger.info(f"Processed {processed} out of {len(self.quadkeys_to_process)} quadkeys")
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def write(self, data_object_id: str):
        self.logger.info(f"Writing {data_object_id} data object")
        self.output_data_objects[data_object_id].write()
        return None

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # process transportaion
        transportation_sdf = self.get_raw_overture_data(self.transportation, ["class", "geometry", "subtype"], 2)
        self.output_data_objects[self.transportation].df = transportation_sdf

        self.write(self.transportation)

        # process landuse
        landuse_cols_to_select = ["subtype", "geometry"]

        landuse_sdf = self.get_raw_overture_data(self.landuse, landuse_cols_to_select, 3)
        self.output_data_objects[self.landuse].df = landuse_sdf

        self.write(self.landuse)

        # process landcover

        landcover_sdf = self.get_raw_overture_data(self.landcover, landuse_cols_to_select, 3)
        self.output_data_objects[self.landcover].df = landcover_sdf

        self.write(self.landcover)

        # process water
        water_sdf = self.get_raw_overture_data(self.water, landuse_cols_to_select, 3)
        water_sdf = water_sdf.withColumn("subtype", F.lit("water"))
        self.output_data_objects[self.water].df = water_sdf

        self.write(self.water)

        # process buildings
        buildings_sdf = self.get_raw_overture_data(self.buildings, landuse_cols_to_select, 3)
        self.output_data_objects[self.buildings].df = buildings_sdf

        self.write(self.buildings)

    def get_raw_overture_data(self, data_type: str, cols_to_select, geometry_type: int) -> DataFrame:
        """
        Retrieves and processes Overture Maps raw data for a specific land use type.

        This function filters input data objects based on the specified data type and optional filter types,
        fixes the polygon geometry, and projects the data to a specific CRS.
        If the persist parameter is set to True, the resulting DataFrame is persisted in memory and disk.

        Args:
            data_type (str): The type of land use data to retrieve.
            filter_types (list, optional): A list of subtypes to filter the data by. Each subtype is a string.
                If None, no filtering is performed. Defaults to None.
            persist (bool, optional): Whether to persist the resulting DataFrame in memory and disk. Defaults to True.

        Returns:
            DataFrame: A DataFrame containing the processed land use data.
            The DataFrame includes a subtype column and a geometry column.
        """

        sdf = self.filter_input_data_objects(data_type, cols_to_select)

        sdf = utils.project_to_crs(sdf, 4326, 3035)
        sdf = utils.fix_geometry(sdf, geometry_type)
        sdf = sdf.withColumn(ColNames.quadkey, F.lit(self.current_quadkey))

        return sdf

    def filter_input_data_objects(self, data_type: str, required_columns: List[str]) -> DataFrame:
        """
        Filters and processes input Overture Maps data based on the specified data type and columns.

        This function selects the required columns from the input data objects,
        filters the data to the current processing iteration extent, and cuts the data to the general extent.
        If the data type is "landcover", "landuse", or "transportation", it further filters the data by the specified subtypes.
        If the data type is not "transportation", it filters out invalid polygons.

        Args:
            data_type (str): The type of data to filter and process. "landcover", "landuse", "transportation", "buildings", or "water".
            required_columns (list): A list of column names to select from the data. Each column name is a string.
            category_col (str): The name of the category column to filter by when the data type is "landcover", "landuse", or "transportation".
            subtypes (list, optional): A list of subtypes to filter the data by when the data type is "landcover", "landuse", or "transportation".
                Each subtype is a string. If None, no subtype filtering is performed. Defaults to None.

        Returns:
            DataFrame: A DataFrame containing the filtered and processed data.
        """

        do_sdf = self.input_data_objects[data_type].df
        do_sdf = do_sdf.select(*required_columns)
        do_sdf = utils.cut_geodata_to_extent(do_sdf, self.current_extent, 4326)

        return do_sdf
