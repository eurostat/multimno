""" """

import operator
from functools import reduce
from typing import Dict, List

from sedona.sql import st_functions as STF
from sedona.sql import st_aggregates as STA
from sedona.sql import st_predicates as STP

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
from multimno.core.data_objects.bronze.bronze_buildings_data_object import (
    BronzeBuildingsDataObject,
)

from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_LANDING_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
import multimno.core.quadkey_utils as quadkey_utils


class OvertureDataTransformation(Component):
    """ """

    COMPONENT_ID = "OvertureDataTransformation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.transportation_reclass_map = self.config.geteval(self.COMPONENT_ID, "transportation_reclass_map")
        self.landuse_reclass_map = self.config.geteval(self.COMPONENT_ID, "landuse_landcover_reclass_map")
        self.bulding_reclass_map = self.config.geteval(self.COMPONENT_ID, "buildings_reclass_map")

        self.landuse_filter_subtypes = self.config.geteval(self.COMPONENT_ID, "landuse_filter_subtypes")
        self.landcover_filter_subtypes = self.config.geteval(self.COMPONENT_ID, "landcover_filter_subtypes")
        self.transportation_filter_subtypes = self.config.geteval(self.COMPONENT_ID, "transportation_filter_subtypes")
        self.buildings_filter_subtypes = self.config.geteval(self.COMPONENT_ID, "buildings_filter_subtypes")
        self.quadkey_processing_batch = self.config.getint(self.COMPONENT_ID, "quadkey_processing_batch")
        self.number_of_self_clip_iterations = 2

        self.water_filter_area_threshold_m2 = self.config.getint(self.COMPONENT_ID, "water_filter_area_threshold_m2")
        self.landuse_filter_area_threshold_m2 = self.config.getint(
            self.COMPONENT_ID, "landuse_filter_area_threshold_m2"
        )
        self.buildings_filter_area_threshold_m2 = self.config.getint(
            self.COMPONENT_ID, "buildings_filter_area_threshold_m2"
        )
        self.landcover_filter_area_threshold_m2 = self.config.getint(
            self.COMPONENT_ID, "landcover_filter_area_threshold_m2"
        )

        self.extent = self.config.geteval(self.COMPONENT_ID, "extent")
        self.quadkey_partition_level = self.config.getint(self.COMPONENT_ID, "quadkey_partition_level")
        self.quadkeys_to_process = self.config.geteval(self.COMPONENT_ID, "quadkeys_to_process")
        self.repartition_factor = self.config.getint(self.COMPONENT_ID, "repartition_factor")

    def initalize_data_objects(self):

        self.transportation = "transportation"
        self.landuse = "landuse"
        self.landcover = "landcover"
        self.buildings = "buildings"
        self.water = "water"

        transportation_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "transportation_data_landing")
        landuse_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "landuse_data_landing")
        landcover_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "landcover_data_landing")
        buildings_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "buildings_data_landing")
        water_do_path = self.config.get(CONFIG_LANDING_PATHS_KEY, "water_data_landing")

        self.input_data_objects = {}
        self.input_data_objects[self.transportation] = LandingGeoParquetDataObject(self.spark, transportation_do_path)
        self.input_data_objects[self.buildings] = LandingGeoParquetDataObject(self.spark, buildings_do_path)
        self.input_data_objects[self.landcover] = LandingGeoParquetDataObject(self.spark, landcover_do_path)
        self.input_data_objects[self.landuse] = LandingGeoParquetDataObject(self.spark, landuse_do_path)
        self.input_data_objects[self.water] = LandingGeoParquetDataObject(self.spark, water_do_path)

        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")

        transportation_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "transportation_data_bronze")
        landuse_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "landuse_data_bronze")
        buildings_do_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "buildings_data_bronze")

        self.output_data_objects = {}

        self.output_data_objects[BronzeTransportationDataObject.ID] = BronzeTransportationDataObject(
            self.spark,
            transportation_do_path,
        )

        self.output_data_objects[BronzeLanduseDataObject.ID] = BronzeLanduseDataObject(self.spark, landuse_do_path)
        self.output_data_objects[BronzeBuildingsDataObject.ID] = BronzeBuildingsDataObject(
            self.spark, buildings_do_path
        )

        if self.clear_destination_directory:
            for do in self.output_data_objects.values():
                self.logger.info(f"Clearing {do.default_path}")
                delete_file_or_folder(self.spark, do.default_path)

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        if len(self.quadkeys_to_process) == 0:
            self.quadkeys_to_process = quadkey_utils.get_quadkeys_for_bbox(self.extent, self.quadkey_partition_level)

        self.logger.info("quadkeys to process: ")
        self.logger.info(f"{self.quadkeys_to_process}")

        # generate quadkey batches
        self.quadkey_batches = self.generate_batches(self.quadkeys_to_process, self.quadkey_processing_batch)
        self.logger.info(f"Will be processrd in: {len(self.quadkey_batches)} parts")
        processed = 0
        for quadkey_batch in self.quadkey_batches:
            self.logger.info(f"Processing quadkeys {quadkey_batch}")
            self.current_quadkey_batch = quadkey_batch
            self.transform()
            self.spark.catalog.clearCache()
            processed += 1
            self.logger.info(f"Processed {processed} out of {len(self.quadkey_batches)} batches")
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def generate_batches(self, elements_list, batch_size):
        """
        Generates batches of elements from list.
        """
        return [elements_list[i : i + batch_size] for i in range(0, len(elements_list), batch_size)]

    def write(self, data_object_id: str):
        self.logger.info(f"Writing {data_object_id} data object")
        self.output_data_objects[data_object_id].write()
        return None

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        # process transportaion
        transportation_sdf = self.filter_data(self.transportation, self.transportation_filter_subtypes)
        transportation_sdf = self.reclassify(
            transportation_sdf,
            self.transportation_reclass_map,
            "subtype",
            ColNames.category,
            "unknown",
        )
        transportation_sdf = utils.apply_schema_casting(transportation_sdf, BronzeTransportationDataObject.SCHEMA)
        transportation_sdf = transportation_sdf.repartition(*BronzeTransportationDataObject.PARTITION_COLUMNS)
        self.output_data_objects[BronzeTransportationDataObject.ID].df = transportation_sdf
        self.write(BronzeTransportationDataObject.ID)

        # get higher resolution quadkeys for dissolving geometries
        child_quadkeys = [quadkey_utils.get_children_quadkeys(quadkey, 14) for quadkey in self.current_quadkey_batch]
        child_quadkeys = reduce(operator.add, child_quadkeys, [])

        quadkeys_mask_sdf = quadkey_utils.quadkeys_to_extent_dataframe(self.spark, child_quadkeys, 3035)
        quadkeys_mask_sdf = quadkeys_mask_sdf.withColumnRenamed(ColNames.quadkey, "mask_quadkey")

        quadkeys_mask_sdf = quadkeys_mask_sdf.persist()
        quadkeys_mask_sdf.count()
        quadkeys_mask_sdf = F.broadcast(quadkeys_mask_sdf)

        # process landuse
        landuse_cols_to_select = ["subtype", "geometry", "quadkey"]

        landuse_sdf = self.filter_data(
            self.landuse, self.landuse_filter_subtypes, self.landuse_filter_area_threshold_m2
        )

        landuse_sdf = utils.merge_geom_within_mask_geom(
            landuse_sdf,
            quadkeys_mask_sdf,
            ["subtype", ColNames.quadkey, "mask_quadkey"],
            ColNames.geometry,
        ).select("subtype", ColNames.geometry, ColNames.quadkey)

        # self clip is neded to get rid of overlapping geometries to make sure that only one landuse category is present in each area
        # multiple iterations are needed as only one overlap can be clipped at a time
        for i in range(self.number_of_self_clip_iterations):
            landuse_sdf = utils.clip_polygons_with_mask_polygons(landuse_sdf, landuse_sdf, landuse_cols_to_select, True)
            landuse_sdf = landuse_sdf.persist()
            landuse_sdf.count()
            self.logger.info(f"Landuse prepared {i + 1}")

        landcover_sdf = self.filter_data(
            self.landcover, self.landcover_filter_subtypes, self.landcover_filter_area_threshold_m2
        )

        landcover_sdf = utils.merge_geom_within_mask_geom(
            landcover_sdf,
            quadkeys_mask_sdf,
            ["subtype", ColNames.quadkey, "mask_quadkey"],
            ColNames.geometry,
        ).select("subtype", ColNames.geometry, ColNames.quadkey)

        landcover_sdf = utils.clip_polygons_with_mask_polygons(landcover_sdf, landuse_sdf, landuse_cols_to_select)
        landcover_sdf = landcover_sdf.persist()
        landcover_sdf.count()
        self.logger.info("Landcover prepared")

        landuse_sdf = utils.clip_polygons_with_mask_polygons(landuse_sdf, landcover_sdf, landuse_cols_to_select)

        landuse_landcover_sdf = landcover_sdf.union(landuse_sdf)
        landuse_landcover_sdf = landuse_landcover_sdf.persist()
        landuse_landcover_sdf.count()
        self.logger.info("Landuse and landcover merged")

        # process water
        water_sdf = self.filter_data(self.water, ["water"], self.water_filter_area_threshold_m2)

        water_sdf = utils.merge_geom_within_mask_geom(
            water_sdf,
            quadkeys_mask_sdf,
            ["subtype", ColNames.quadkey, "mask_quadkey"],
            ColNames.geometry,
        ).select("subtype", ColNames.geometry, ColNames.quadkey)

        # combine landuse with water
        landuse_landcover_sdf = utils.clip_polygons_with_mask_polygons(
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

        landuse_cover_water_sdf = landuse_cover_water_sdf.persist()
        landuse_cover_water_sdf.count()
        self.logger.info("Landuse, landcover and water merged")

        # add buildings data to full landuse
        buildings_sdf = self.filter_data(self.buildings, self.buildings_filter_subtypes)
        buildings_sdf = self.reclassify(
            buildings_sdf,
            self.bulding_reclass_map,
            "subtype",
            ColNames.category,
            "other_builtup",
        )

        # slightly buffer buildings to merge small geometries together
        # buildings_buff_sdf = buildings_sdf.withColumn(ColNames.geometry, STF.ST_Buffer(ColNames.geometry, 2))

        merged_buildings_sdf = utils.merge_geom_within_mask_geom(
            buildings_sdf,
            quadkeys_mask_sdf,
            [ColNames.category, ColNames.quadkey, "mask_quadkey"],
            ColNames.geometry,
        ).select(ColNames.category, ColNames.geometry, ColNames.quadkey)

        # filter out very small buildings
        merged_buildings_sdf = merged_buildings_sdf.filter(
            STF.ST_Area(ColNames.geometry) > self.buildings_filter_area_threshold_m2
        )
        merged_buildings_sdf = merged_buildings_sdf.persist()
        merged_buildings_sdf.count()
        self.logger.info("Buildings merged")

        # combine landuse with buildings
        landuse_cover_water_sdf = utils.clip_polygons_with_mask_polygons(
            landuse_cover_water_sdf,
            merged_buildings_sdf,
            [ColNames.category, ColNames.geometry, ColNames.quadkey],
        )

        landuse_cover_water_sdf = landuse_cover_water_sdf.union(merged_buildings_sdf)

        landuse_cover_water_sdf = utils.apply_schema_casting(landuse_cover_water_sdf, BronzeLanduseDataObject.SCHEMA)
        landuse_cover_water_sdf = landuse_cover_water_sdf.repartition(*BronzeLanduseDataObject.PARTITION_COLUMNS)
        self.output_data_objects[BronzeLanduseDataObject.ID].df = landuse_cover_water_sdf
        self.write(BronzeLanduseDataObject.ID)

        # process buildings
        buildings_sdf = utils.apply_schema_casting(buildings_sdf, BronzeBuildingsDataObject.SCHEMA)
        buildings_sdf = buildings_sdf.repartition(*BronzeBuildingsDataObject.PARTITION_COLUMNS)
        self.output_data_objects[BronzeBuildingsDataObject.ID].df = buildings_sdf
        self.write(BronzeBuildingsDataObject.ID)

    def filter_data(self, data_type, subtypes_to_filter, area_filter_threshold_m2=None) -> DataFrame:
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

        do_sdf = self.input_data_objects[data_type].df
        do_sdf = do_sdf.repartition(self.repartition_factor)
        # TODO move to data ingestion
        if data_type == "transportation":
            do_sdf = do_sdf.filter(F.col("subtype").isin(["road", "rail"]))
            # assign subtype railroad to rail
            do_sdf = do_sdf.withColumn(
                "class", F.when(F.col("subtype") == "rail", "railroad").otherwise(F.col("class"))
            ).drop("subtype")
            do_sdf = do_sdf.withColumnRenamed("class", "subtype")

        # TODO make subtype column a parameter
        do_sdf = do_sdf.withColumn("subtype", F.coalesce(F.col("subtype"), F.lit("unknown")))
        do_sdf = do_sdf.filter(F.col("subtype").isin(subtypes_to_filter))

        mask_sdf = quadkey_utils.quadkeys_to_extent_dataframe(self.spark, self.current_quadkey_batch, 3035)
        do_sdf = self.cut_geoms_with_mask_polygons(do_sdf, mask_sdf, ["subtype", ColNames.geometry], [ColNames.quadkey])

        do_sdf = do_sdf.withColumn(
            ColNames.geometry, F.explode(STF.ST_Dump(ColNames.geometry)).alias(ColNames.geometry)
        )
        if area_filter_threshold_m2:
            do_sdf = do_sdf.filter(STF.ST_Area(ColNames.geometry) > area_filter_threshold_m2)

        return do_sdf

    def cut_geoms_with_mask_polygons(
        self,
        sdf: DataFrame,
        mask_sdf: DataFrame,
        orig_cols_to_select: List[str],
        cut_cols_to_select: List[str],
        cut_buffer: int = 100,
    ) -> DataFrame:
        """
        Cuts geometries in a DataFrame with mask polygons from another DataFrame.
        """
        cut_sdf = sdf.select(*orig_cols_to_select).join(
            mask_sdf.select(F.col(ColNames.geometry).alias("cut_geometry"), *cut_cols_to_select),
            on=STP.ST_Intersects(ColNames.geometry, "cut_geometry"),
        )

        cut_sdf = cut_sdf.withColumn(
            ColNames.geometry,
            STF.ST_Intersection(cut_sdf[ColNames.geometry], STF.ST_Buffer(cut_sdf["cut_geometry"], cut_buffer)),
        ).drop("cut_geometry")

        return cut_sdf

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

        return sdf.select(ColNames.category, ColNames.geometry, ColNames.quadkey).drop(class_column)
