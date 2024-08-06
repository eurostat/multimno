"""

"""

import math
from typing import Dict, List, Optional, Tuple

from sedona.sql import st_functions as STF
from sedona.sql import st_aggregates as STA

import pyspark.sql.functions as F
from pyspark import StorageLevel
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

from multimno.core.spark_session import delete_file_or_folder
from multimno.core.component import Component
from multimno.core.data_objects.landing.landing_geoparquet_data_object import (
    LandingGeoParquetDataObject,
)
from multimno.core.data_objects.bronze.bronze_landuse_data_object import (
    BronzeLanduseDataObject,
)
from multimno.core.data_objects.bronze.bronze_transportation_data_object import (
    BronzeTransportationDataObject,
)

from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils


class OvertureDataIngestion(Component):
    """ """

    COMPONENT_ID = "OvertureDataIngestion"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.transportation_reclass_map = self.config.geteval(
            OvertureDataIngestion.COMPONENT_ID, "transportation_reclass_map"
        )
        self.landuse_reclass_map = self.config.geteval(
            OvertureDataIngestion.COMPONENT_ID, "landuse_landcover_reclass_map"
        )
        self.bulding_reclass_map = self.config.geteval(OvertureDataIngestion.COMPONENT_ID, "buildings_reclass_map")

        self.landuse_filter_subtypes = self.config.geteval(
            OvertureDataIngestion.COMPONENT_ID, "landuse_filter_subtypes"
        )
        self.landcover_filter_subtypes = self.config.geteval(
            OvertureDataIngestion.COMPONENT_ID, "landcover_filter_subtypes"
        )

        self.transportation_filter_subtypes = self.config.geteval(
            OvertureDataIngestion.COMPONENT_ID, "transportation_filter_subtypes"
        )

        self.extent = self.config.geteval(OvertureDataIngestion.COMPONENT_ID, "extent")

        self.spatial_repartition_size_rows = self.config.getint(
            OvertureDataIngestion.COMPONENT_ID, "spatial_repartition_size_rows"
        )

        self.min_partition_quadkey_level = self.config.getint(
            OvertureDataIngestion.COMPONENT_ID, "min_partition_quadkey_level"
        )

        self.max_partition_quadkey_level = self.config.getint(
            OvertureDataIngestion.COMPONENT_ID, "max_partition_quadkey_level"
        )

        self.extraction_quadkey_level = self.config.getint(
            OvertureDataIngestion.COMPONENT_ID, "extraction_quadkey_level"
        )

        self.use_buildings = self.config.getboolean(OvertureDataIngestion.COMPONENT_ID, "use_buildings")

        self.quadkey_udf = F.udf(utils.latlon_to_quadkey)

    def initalize_data_objects(self):

        overture_url = self.config.get(OvertureDataIngestion.COMPONENT_ID, "overture_url")

        transportation_url = overture_url + "/theme=transportation/type=segment"

        buildings_url = overture_url + "/theme=buildings/type=building"

        landcover_url = overture_url + "/theme=base/type=land"
        landuse_url = overture_url + "/theme=base/type=land_use"
        water_url = overture_url + "/theme=base/type=water"

        self.input_data_objects = {}
        self.input_data_objects["transportation"] = LandingGeoParquetDataObject(self.spark, transportation_url, [])
        self.input_data_objects["buildings"] = LandingGeoParquetDataObject(self.spark, buildings_url, [])
        self.input_data_objects["landcover"] = LandingGeoParquetDataObject(self.spark, landcover_url, [])
        self.input_data_objects["landuse"] = LandingGeoParquetDataObject(self.spark, landuse_url, [])
        self.input_data_objects["water"] = LandingGeoParquetDataObject(self.spark, water_url, [])

        self.clear_destination_directory = self.config.getboolean(
            OvertureDataIngestion.COMPONENT_ID, "clear_destination_directory"
        )

        transportation_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "transportation_data_bronze")

        landuse_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "landuse_data_bronze")

        self.output_data_objects = {}

        self.output_data_objects[BronzeTransportationDataObject.ID] = BronzeTransportationDataObject(
            self.spark,
            transportation_do_path,
            [ColNames.year, ColNames.month, ColNames.day, ColNames.quadkey],
        )
        self.output_data_objects[BronzeLanduseDataObject.ID] = BronzeLanduseDataObject(
            self.spark,
            landuse_do_path,
            [ColNames.year, ColNames.month, ColNames.day, ColNames.quadkey],
        )

        if self.clear_destination_directory:
            for do in self.output_data_objects.values():
                self.logger.info(f"Clearing {do.default_path}")
                delete_file_or_folder(self.spark, do.default_path)

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        quadkeys = utils.get_quadkeys_for_bbox(self.extent, self.extraction_quadkey_level)

        self.logger.info(f"Extraction will be done in {len(quadkeys)} parts.")
        for quadkey in quadkeys:
            self.logger.info(f"Processing quadkey {quadkey}")
            self.current_extent = utils.quadkey_to_extent(quadkey)
            self.current_quadkey = quadkey
            self.transform()
            self.write()
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # process transportaion
        transportation_sdf = self.get_transportation_data()
        transportation_sdf = transportation_sdf.withColumns(
            {
                ColNames.year: F.year(F.current_date()),
                ColNames.month: F.month(F.current_date()),
                ColNames.day: F.day(F.current_date()),
            }
        )
        transportation_sdf = self.apply_schema_casting(transportation_sdf, BronzeTransportationDataObject.SCHEMA)

        self.output_data_objects[BronzeTransportationDataObject.ID].df = transportation_sdf

        # process landuse
        landuse_cols_to_select = ["subtype", "geometry"]

        landcover_sdf = self.get_raw_data_for_landuse(
            "landcover",
            landuse_cols_to_select,
            self.landcover_filter_subtypes,
            persist=True,
        )

        landuse_sdf = self.get_raw_data_for_landuse(
            "landuse",
            landuse_cols_to_select,
            self.landuse_filter_subtypes,
            persist=True,
        )

        # combine landcover and landuse
        landcover_sdf = utils.cut_polygons_with_mask_polygons(landcover_sdf, landuse_sdf, landuse_cols_to_select)

        landcover_sdf = landcover_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landcover_sdf.count()

        landuse_sdf = utils.cut_polygons_with_mask_polygons(landuse_sdf, landcover_sdf, landuse_cols_to_select)

        landuse_sdf = landuse_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landuse_sdf.count()

        landuse_sdf = utils.cut_polygons_with_mask_polygons(landuse_sdf, landuse_sdf, landuse_cols_to_select, True)

        landuse_sdf = landuse_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landuse_sdf.count()

        landuse_landcover_sdf = landcover_sdf.union(landuse_sdf)

        # blow up too big landuse polygons
        # TODO: asses feasibility of this
        # TODO: make vertices number parameter
        # TODO: introduce cut by qaudkeys
        # landuse_landcover_sdf = landuse_landcover_sdf.withColumn(ColNames.geometry, STF.ST_SubDivideExplode(ColNames.geometry, 1000))

        landuse_landcover_sdf = landuse_landcover_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landuse_landcover_sdf.count()
        self.logger.info(f"merged landuse and landcover")

        water_sdf = self.get_raw_data_for_landuse("water", landuse_cols_to_select, persist=True)
        water_sdf = water_sdf.withColumn("subtype", F.lit("water"))

        # combine landuse with water
        landuse_landcover_sdf = utils.cut_polygons_with_mask_polygons(
            landuse_landcover_sdf, water_sdf, landuse_cols_to_select
        )
        landuse_cover_water_sdf = landuse_landcover_sdf.union(water_sdf)

        # reclassify to config categories
        landuse_cover_water_sdf = self.reclassify(
            landuse_cover_water_sdf,
            self.landuse_reclass_map,
            "subtype",
            ColNames.category,
            "open_area",
        )

        landuse_cover_water_sdf = utils.fix_geometry(landuse_cover_water_sdf, 3)

        landuse_cover_water_sdf = landuse_cover_water_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        landuse_cover_water_sdf.count()
        self.logger.info(f"merged landuse and water")

        # add buildings data to full landuse
        if self.use_buildings:

            buildings_sdf = self.get_raw_data_for_landuse("buildings", ["class", "geometry"], persist=True)

            buildings_sdf = self.reclassify(
                buildings_sdf,
                self.bulding_reclass_map,
                "class",
                ColNames.category,
                "other_builtup",
            )

            buildings_sdf = self.merge_buildings_by_quadkey(buildings_sdf, 3035, 16)
            buildings_sdf = buildings_sdf.drop("quadkey")
            buildings_sdf = buildings_sdf.persist(StorageLevel.MEMORY_AND_DISK)
            buildings_sdf.count()
            self.logger.info(f"merged buildings")

            # combine landuse with buildings
            landuse_cover_water_sdf = utils.cut_polygons_with_mask_polygons(
                landuse_cover_water_sdf,
                buildings_sdf,
                [ColNames.category, ColNames.geometry],
            )

            landuse_cover_water_sdf = utils.fix_geometry(landuse_cover_water_sdf, 3, ColNames.geometry)

            landuse_cover_water_sdf = landuse_cover_water_sdf.union(buildings_sdf)

        landuse_cover_water_sdf = utils.assign_quadkey(landuse_cover_water_sdf, 3035, self.max_partition_quadkey_level)

        # TODO: Figure out if this optimization is ever needed and how to implement it properly
        # landuse_cover_water_sdf = utils.coarsen_quadkey_to_partition_size(landuse_cover_water_sdf,
        #                                                                   self.spatial_repartition_size_rows,
        #                                                                   self.min_partition_quadkey_level)

        landuse_cover_water_sdf = landuse_cover_water_sdf.withColumns(
            {
                ColNames.year: F.year(F.current_date()),
                ColNames.month: F.month(F.current_date()),
                ColNames.day: F.day(F.current_date()),
            }
        )
        landuse_cover_water_sdf = landuse_cover_water_sdf.orderBy("quadkey")
        landuse_cover_water_sdf = landuse_cover_water_sdf.repartition("quadkey")

        landuse_cover_water_sdf = self.apply_schema_casting(landuse_cover_water_sdf, BronzeLanduseDataObject.SCHEMA)

        self.output_data_objects[BronzeLanduseDataObject.ID].df = landuse_cover_water_sdf

    def get_transportation_data(self) -> DataFrame:
        """
        Processes and returns Overture Maps transportation data.

        This function filters input data objects based on the transportation class and specified subtypes,
        reclassifies the transportation data based on a predefined map, and selects specific columns for further processing.
        It then fixes the line geometry, assigns quadkeys, projects the data to a specific CRS,
        orders the data by quadkey, and repartitions the data based on quadkey.

        Returns:
            DataFrame: A DataFrame containing the processed transportation data.
            The DataFrame includes a category column, a geometry column, and a quadkey column.
        """
        # function implementation...

        transportation_sdf = self.filter_input_data_objects(
            "transportation",
            ["class", "geometry"],
            "class",
            self.transportation_filter_subtypes,
        )
        transportation_sdf = self.reclassify(
            transportation_sdf,
            self.transportation_reclass_map,
            "class",
            ColNames.category,
        )
        transportation_sdf = transportation_sdf.select(ColNames.category, ColNames.geometry).drop("subtype")

        # transportation_sdf.persist(StorageLevel.MEMORY_AND_DISK)
        # transportation_sdf.count()
        # self.logger.info(f"got transportation")
        transportation_sdf = utils.project_to_crs(transportation_sdf, 4326, 3035)
        transportation_sdf = utils.fix_geometry(transportation_sdf, 2)
        transportation_sdf = utils.assign_quadkey(transportation_sdf, 3035, self.max_partition_quadkey_level)

        # TODO: Figure out how to implement this
        # transportation_sdf = utils.coarsen_quadkey_to_partition_size(transportation_sdf, self.spatial_repartition_size_rows, self.min_partition_quadkey_level)
        transportation_sdf = transportation_sdf.orderBy("quadkey")
        transportation_sdf = transportation_sdf.repartition("quadkey")

        return transportation_sdf

    def get_raw_data_for_landuse(
        self,
        data_type: str,
        cols_to_select,
        filter_types: Optional[List[str]] = None,
        persist: bool = True,
    ) -> DataFrame:
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
        # function implementation...

        sdf = self.filter_input_data_objects(data_type, cols_to_select, "subtype", filter_types)

        sdf = utils.project_to_crs(sdf, 4326, 3035)
        sdf = utils.fix_geometry(sdf, 3, ColNames.geometry)

        if persist:
            sdf = sdf.persist(StorageLevel.MEMORY_AND_DISK)
            sdf.count()
            self.logger.info(f"got {data_type}")

        return sdf

    def merge_buildings_by_quadkey(self, sdf: DataFrame, crs: int, quadkey_level: int = 16) -> DataFrame:
        """
        Merges building polygons within each quadkey.

        This function assigns a quadkey to each building polygon in the input DataFrame,
        then groups the DataFrame by quadkey and building category, and merges the polygons within each group.

        Args:
            sdf (DataFrame): A DataFrame containing the building polygons.
            crs (int): The coordinate reference system of the input geometries.
            qadkey_level (int, optional): The zoom level to use when assigning quadkeys. Defaults to 16.

        Returns:
            DataFrame: A DataFrame containing the merged building polygons.
        """

        sdf = utils.assign_quadkey(sdf, crs, quadkey_level)

        # TODO: test more if this would make any difference

        # sdf = utils.coarsen_quadkey_to_partition_size(
        #     sdf, self.spatial_repartition_size_rows, 10
        # )

        # sdf = sdf.withColumn("quadkey_merge", F.col("quadkey").substr(1, self.max_partition_quadkey_level))
        # sdf = sdf.repartition("quadkey_merge").drop("quadkey_merge")

        sdf = sdf.groupBy("quadkey", "category").agg(STA.ST_Union_Aggr(ColNames.geometry).alias(ColNames.geometry))

        return sdf

    def filter_input_data_objects(
        self,
        data_type: str,
        required_columns: List[str],
        category_col: str,
        subtypes: Optional[List[str]] = None,
    ) -> DataFrame:
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
        # function implementation...

        do_sdf = self.input_data_objects[data_type].df.select(*required_columns)
        do_sdf = self.filter_data_to_extent(do_sdf, self.extent)
        do_sdf = utils.cut_geodata_to_extent(do_sdf, self.current_extent, 4326)
        do_sdf = self.filter_to_quadkey(do_sdf, self.current_quadkey, self.extraction_quadkey_level)
        if data_type in ["landcover", "landuse", "transportation"]:
            do_sdf = do_sdf.filter(F.col(category_col).isin(subtypes))

        if data_type not in ["transportation"]:
            do_sdf = self.filter_polygons(do_sdf)

        return do_sdf

    @staticmethod
    def filter_to_quadkey(sdf: DataFrame, current_quadkey: str, quadkey_level: int) -> DataFrame:
        """
        Filters a DataFrame to include only rows with polygon geometries.

        Args:
            sdf (DataFrame): A DataFrame that includes a geometry column.

        Returns:
            DataFrame: A DataFrame containing only the rows from the input DataFrame where the geometry is a polygon or multipolygon.
        """

        sdf = utils.assign_quadkey(sdf, 4326, quadkey_level)

        return sdf.filter(F.col("quadkey") == F.lit(current_quadkey)).drop("quadkey")

    @staticmethod
    def filter_polygons(sdf: DataFrame) -> DataFrame:
        """
        Filters a DataFrame to include only rows with polygon geometries.

        Args:
            sdf (DataFrame): A DataFrame that includes a geometry column.

        Returns:
            DataFrame: A DataFrame containing only the rows from the input DataFrame where the geometry is a polygon or multipolygon.
        """
        return sdf.filter(STF.ST_GeometryType(F.col(ColNames.geometry)).like("%Polygon%"))

    @staticmethod
    def apply_schema_casting(sdf: DataFrame, schema: StructType) -> DataFrame:
        """
        This function takes a DataFrame and a schema, and applies the schema to the DataFrame.
        It selects the columns in the DataFrame that are in the schema, and casts each column to the type specified in the schema.

        Args:
            sdf (DataFrame): The DataFrame to apply the schema to.
            schema (StructType): The schema to apply to the DataFrame.

        Returns:
            DataFrame: A new DataFrame that includes the same rows as the input DataFrame,
            but with the columns cast to the types specified in the schema.
        """

        sdf = sdf.select(*[F.col(field.name) for field in schema.fields])
        for field in schema.fields:
            sdf = sdf.withColumn(field.name, F.col(field.name).cast(field.dataType))

        return sdf

    @staticmethod
    def filter_data_to_extent(sdf: DataFrame, extent: Tuple[float, float, float, float]) -> DataFrame:
        """
        Filters an Overture Maps DataFrame to include only rows within a specified extent.

        Args:
            sdf (DataFrame): The DataFrame to filter.
            extent (tuple): A tuple representing the extent. The tuple contains four elements:
                (west, south, east, north), which are the western, southern, eastern, and northern bounds of the WGS84 extent.

        Returns:
            DataFrame: A DataFrame containing only the rows from the input DataFrame where the bbox is within the extent.
        """

        sdf = sdf.filter(
            ((F.col("bbox")["xmin"]).between(F.lit(extent[0]), F.lit(extent[2])))
            & ((F.col("bbox")["ymin"]).between(F.lit(extent[1]), F.lit(extent[3])))
        )
        return sdf

    @staticmethod
    def reclassify(
        sdf: DataFrame,
        reclass_map: Dict[str, List[str]],
        class_column: str,
        reclass_column: str,
        default_reclass: str = "unknown",
    ) -> DataFrame:
        """
        Reclassifies a column in a DataFrame based on a reclassification map.

        This function takes a DataFrame, a reclassification map, and the names of a class column and a reclassification column.
        It creates a new column in the DataFrame by reclassifying the values in the class column based on the reclassification map.
        If a value in the class column is not in the reclassification map, it is classified as the default reclassification.

        Args:
            sdf (DataFrame): The DataFrame to reclassify.
            reclass_map (dict): The reclassification map. The keys are the reclassified classes, and the values are lists of original classes.
            class_column (str): The name of the column in the DataFrame to reclassify.
            reclass_column (str): The name of the new column to create with the reclassified classes.
            default_reclass (str, optional): The class to assign to values in the class column that are not in the map. Defaults to "unknown".

        Returns:
            DataFrame: A DataFrame containing the same rows as the input DataFrame, but with the class column replaced by the reclassification column.
        """
        # function implementation...

        keys = list(reclass_map.keys())
        reclass_expr = F.when(F.col(class_column).isin(reclass_map[keys[0]]), keys[0])

        for key in keys[1:]:
            reclass_expr = reclass_expr.when(F.col(class_column).isin(reclass_map[key]), key)

        reclass_expr = reclass_expr.otherwise(default_reclass)

        sdf = sdf.withColumn(reclass_column, reclass_expr)

        return sdf.select(ColNames.category, ColNames.geometry).drop(class_column)
