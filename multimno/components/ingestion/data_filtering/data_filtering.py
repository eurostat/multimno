"""
Module that cleans RAW MNO Event and Network Topology data.
"""

from typing import List, Dict
from datetime import datetime, timedelta, time, date
from functools import reduce
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import (
    BronzeNetworkDataObject,
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
from multimno.core.constants.error_types import ErrorTypes
from multimno.core.constants.transformations import Transformations
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

        self.do_bbox_filtering = self.config.getboolean(
            self.COMPONENT_ID,
            "do_bbox_filtering",
        )

        self.bbox_filtering_variant = self.config.getint(
            self.COMPONENT_ID,
            "bbox_filtering_variant",
        )

        self.bbox = self.config.geteval(self.COMPONENT_ID, "bbox")

        self.do_device_sampling = self.config.getboolean(
            self.COMPONENT_ID,
            "do_device_sampling",
        )
        
        self.sample_size = self.config.getfloat(
            self.COMPONENT_ID,
            "sample_size",
        )
        self.current_date = None

    def initalize_data_objects(self):
        # Input
        self.input_data_objects = {}

        inputs = {
            "event_data_bronze": BronzeEventDataObject,
            "network_data_bronze": BronzeNetworkDataObject,
        }

        for key, value in inputs.items():
            path = self.config.get(CONFIG_BRONZE_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

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
            self.output_data_objects[value.ID] = value(self.spark, 
                                                       path,
                                                       partition_columns=[ColNames.year, ColNames.month, ColNames.day], 
                                                       mode="append")
    
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
                F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                == F.lit(self.current_date)
            )
        cells_sdf = self.input_data_objects[BronzeNetworkDataObject.ID].df.filter(
                F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                == F.lit(self.current_date)
            )

        if self.do_bbox_filtering:
            if self.bbox_filtering_variant == 1:
                events_sdf, cells_sdf = self.filter_by_bbox(events_sdf, cells_sdf, self.bbox)
            elif self.bbox_filtering_variant == 2:
                events_sdf, cells_sdf = self.filter_by_devices_in_bbox(events_sdf, cells_sdf, self.bbox)

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
    def filter_by_bbox(events_sdf: DataFrame, cells_sdf: DataFrame, bbox: List) -> DataFrame:
        """
        Filters events DataFrame based on bounding box.
        """
        cells_sdf = DataFiltering.filter_cells_bbox(cells_sdf, bbox)
        cell_ids_list = [row[ColNames.cell_id] for row in cells_sdf.select(ColNames.cell_id).collect()]
        events_sdf = events_sdf.filter(events_sdf[ColNames.cell_id].isin(cell_ids_list))
        return events_sdf, cells_sdf
    

    @staticmethod
    def filter_by_devices_in_bbox(events_sdf: DataFrame, cells_sdf: DataFrame, bbox: List) -> DataFrame:
        """
        Filters events DataFrame based on devices in bounding box.
        """
        # Get cells in the bounding box
        cells_in_bbox_sdf = DataFiltering.filter_cells_bbox(cells_sdf, bbox)
        cell_ids_list = [row[ColNames.cell_id] for row in cells_in_bbox_sdf.select(ColNames.cell_id).collect()]
        
        # Filter events based on cell_id being in the cells from the bounding box
        events_in_bbox_sdf = events_sdf.filter(events_sdf[ColNames.cell_id].isin(cell_ids_list))
        
        # Get distinct devices in the filtered events
        devices_in_bbox_sdf = events_in_bbox_sdf.select(ColNames.user_id).distinct()
        
        # Filter original events by the devices in the bounding box
        events_sdf = events_sdf.join(devices_in_bbox_sdf, on=ColNames.user_id, how='inner')
        
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
        events_sdf = events_sdf.join(devices_sdf, on=ColNames.user_id, how='inner')
        
        return events_sdf
    
    