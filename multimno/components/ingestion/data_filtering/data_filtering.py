"""
Module that cleans RAW MNO Event and Network Topology data.
"""

from typing import List
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from sedona.sql import st_functions as STF
from sedona.sql import st_constructors as STC
from sedona.sql import st_predicates as STP
from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import (
    BronzeNetworkDataObject,
)
from multimno.core.data_objects.bronze.bronze_countries_data_object import (
    BronzeCountriesDataObject,
)
from multimno.core.spark_session import (
    check_if_data_path_exists,
    delete_file_or_folder,
)
from multimno.core.settings import (
    CONFIG_BRONZE_PATHS_KEY,
    CONFIG_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats


class DataFiltering(Component):
    """
    Class that filters MNO Event and network topology data
    """

    COMPONENT_ID = "DataFiltering"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

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

        self.do_spatial_filtering = self.config.getboolean(
            self.COMPONENT_ID,
            "do_spatial_filtering",
        )

        self.spatial_filtering_variant = self.config.getint(
            self.COMPONENT_ID,
            "spatial_filtering_variant",
        )

        self.extent = self.config.geteval(self.COMPONENT_ID, "extent")

        self.do_device_sampling = self.config.getboolean(
            self.COMPONENT_ID,
            "do_device_sampling",
        )

        self.sample_size = self.config.getfloat(
            self.COMPONENT_ID,
            "sample_size",
        )

        self.reference_polygon = self.config.get(self.COMPONENT_ID, "reference_polygon")

        self.current_date = None

    def initalize_data_objects(self):
        # Input
        self.input_data_objects = {}

        inputs = {
            "event_data_bronze": BronzeEventDataObject,
            "network_data_bronze": BronzeNetworkDataObject,
        }
        self.spatial_filtering_mask = self.config.get(self.COMPONENT_ID, "spatial_filtering_mask")
        for key, value in inputs.items():
            path = self.config.get(CONFIG_BRONZE_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        if self.spatial_filtering_mask == "polygon":
            self.input_data_objects[BronzeCountriesDataObject.ID] = BronzeCountriesDataObject(
                self.spark,
                self.config.get(CONFIG_BRONZE_PATHS_KEY, "countries_data_bronze"),
            )

        # Output
        bronze_dir = self.config.get(CONFIG_PATHS_KEY, "bronze_dir")
        bronze_dir_sample = self.config.get(CONFIG_PATHS_KEY, "bronze_dir_sample")
        self.clear_destination_directory = self.config.get(self.COMPONENT_ID, "clear_destination_directory")
        self.output_data_objects = {}

        outputs = {
            "event_data_bronze": BronzeEventDataObject,
            "network_data_bronze": BronzeNetworkDataObject,
        }

        for key, value in outputs.items():
            path = self.config.get(CONFIG_BRONZE_PATHS_KEY, key)
            path = path.replace(bronze_dir, bronze_dir_sample)
            if self.clear_destination_directory:
                delete_file_or_folder(self.spark, path)
            self.output_data_objects[value.ID] = value(self.spark, path)

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        for current_date in self.data_period_dates:
            self.current_date = current_date
            self.logger.info(f"Processing {current_date}")
            self.read()
            self.transform()  # Transforms the input_df
            self.write()
            # after each chunk processing clear all Cache to free memory and disk
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        events_sdf = self.input_data_objects[BronzeEventDataObject.ID].df.filter(
            F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(self.current_date)
        )
        cells_sdf = self.input_data_objects[BronzeNetworkDataObject.ID].df.filter(
            F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(self.current_date)
        )
        if self.spatial_filtering_mask == "polygon":
            polygons = self.input_data_objects[BronzeCountriesDataObject.ID].df.filter(
                F.col(ColNames.iso2) == self.reference_polygon
            )
            polygons = utils.project_to_crs(polygons, 3035, 4326)

        if self.do_spatial_filtering:
            if self.spatial_filtering_variant == 1:
                if self.spatial_filtering_mask == "polygon":
                    events_sdf, cells_sdf = self.filter_by_spatial_area(events_sdf, cells_sdf, polygons)
                elif self.spatial_filtering_mask == "extent":
                    events_sdf, cells_sdf = self.filter_by_spatial_area(events_sdf, cells_sdf, bbox=self.extent)
            elif self.spatial_filtering_variant == 2:
                if self.spatial_filtering_mask == "polygon":
                    events_sdf, cells_sdf = self.filter_by_devices_in_spatial_area(events_sdf, cells_sdf, polygons)
                elif self.spatial_filtering_mask == "extent":
                    events_sdf, cells_sdf = self.filter_by_devices_in_spatial_area(
                        events_sdf, cells_sdf, bbox=self.extent
                    )

        if self.do_device_sampling:
            events_sdf = self.sample_devices(events_sdf, self.sample_size)

        events_sdf = utils.apply_schema_casting(events_sdf, BronzeEventDataObject.SCHEMA)
        cells_sdf = utils.apply_schema_casting(cells_sdf, BronzeNetworkDataObject.SCHEMA)

        self.output_data_objects[BronzeNetworkDataObject.ID].df = cells_sdf
        self.output_data_objects[BronzeEventDataObject.ID].df = events_sdf

    @staticmethod
    def filter_cells_bbox(cells_sdf: DataFrame, bbox: List) -> DataFrame:
        """
        Filters cells DataFrame based on bounding box.
        """
        bbox_filter = (
            (F.col(ColNames.longitude) >= bbox[0])
            & (F.col(ColNames.longitude) <= bbox[2])
            & (F.col(ColNames.latitude) >= bbox[1])
            & (F.col(ColNames.latitude) <= bbox[3])
        )
        return cells_sdf.filter(bbox_filter)

    @staticmethod
    def filter_cells_polygon(cells_sdf: DataFrame, polygons_sdf: DataFrame) -> DataFrame:
        """
        Filters cells DataFrame based on polygon.
        """
        cells_sdf = cells_sdf.withColumn(
            ColNames.geometry, STC.ST_Point(ColNames.longitude, ColNames.latitude)
        ).withColumn(ColNames.geometry, STF.ST_SetSRID(ColNames.geometry, 4326))

        cells_sdf = cells_sdf.join(
            polygons_sdf, STP.ST_Intersects(cells_sdf[ColNames.geometry], polygons_sdf[ColNames.geometry]), "inner"
        ).drop(polygons_sdf[ColNames.geometry])

        return cells_sdf

    @staticmethod
    def filter_by_spatial_area(
        events_sdf: DataFrame, cells_sdf: DataFrame, polygons_sdf: DataFrame = None, bbox: List = None
    ) -> DataFrame:
        """
        Filters events DataFrame based on spatial area.
        """
        if polygons_sdf is not None:
            cells_sdf = DataFiltering.filter_cells_polygon(cells_sdf, polygons_sdf)
        elif bbox is not None:
            cells_sdf = DataFiltering.filter_cells_bbox(cells_sdf, bbox)
        cell_ids_list = [row[ColNames.cell_id] for row in cells_sdf.select(ColNames.cell_id).collect()]
        events_sdf = events_sdf.filter(events_sdf[ColNames.cell_id].isin(cell_ids_list))
        return events_sdf, cells_sdf

    @staticmethod
    def filter_by_devices_in_spatial_area(
        events_sdf: DataFrame, cells_sdf: DataFrame, polygons_sdf: DataFrame = None, bbox: List = None
    ) -> DataFrame:
        """
        Filters events DataFrame based on devices in bounding box.
        """
        # Get cells in the bounding box
        if polygons_sdf is not None:
            cells_in_area_sdf = DataFiltering.filter_cells_polygon(cells_sdf, polygons_sdf)
        elif bbox is not None:
            cells_in_area_sdf = DataFiltering.filter_cells_bbox(cells_sdf, bbox)
        cell_ids_list = [row[ColNames.cell_id] for row in cells_in_area_sdf.select(ColNames.cell_id).collect()]

        # Filter events based on cell_id being in the cells from the bounding box
        events_in_bbox_sdf = events_sdf.filter(events_sdf[ColNames.cell_id].isin(cell_ids_list))

        # Get distinct devices in the filtered events
        devices_in_bbox_sdf = events_in_bbox_sdf.select(ColNames.user_id).distinct()

        # Filter original events by the devices in the bounding box
        events_sdf = events_sdf.join(devices_in_bbox_sdf, on=ColNames.user_id, how="inner")

        # Filter cells based on the distinct cell IDs in the filtered events
        all_cells_sdf = events_sdf.select(ColNames.cell_id).distinct()
        all_cell_ids_list = [row[ColNames.cell_id] for row in all_cells_sdf.collect()]

        cells_sdf = cells_sdf.filter(cells_sdf[ColNames.cell_id].isin(all_cell_ids_list))

        return events_sdf, cells_sdf

    @staticmethod
    def sample_devices(events_sdf: DataFrame, sample_size: float) -> DataFrame:
        """
        Samples devices from the events DataFrame.
        """
        devices_sdf = events_sdf.select(ColNames.user_id).distinct()
        devices_sdf = devices_sdf.sample(False, sample_size)

        # Perform an inner join to filter events_sdf by the sampled devices
        events_sdf = events_sdf.join(devices_sdf, on=ColNames.user_id, how="inner")

        return events_sdf
