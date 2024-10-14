"""
This module is responsible for aggregation of the gridded indicators to geographical zones of interest.
"""

from typing import Any, Dict
from sedona.sql import st_predicates as STP
import importlib

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.core.component import Component
from multimno.core.data_objects.data_object import DataObject
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats

CLASS_MAPPING = {
    "present_population": {
        "input": [
            "multimno.core.data_objects.silver.silver_present_population_data_object",
            "SilverPresentPopulationDataObject",
            "present_population_silver",
        ],
        "output": [
            "multimno.core.data_objects.silver.silver_present_population_zone_data_object",
            "SilverPresentPopulationZoneDataObject",
            "present_population_zone_silver",
        ],
    },
    "usual_environments": {
        "input": [
            "multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object",
            "SilverAggregatedUsualEnvironmentsDataObject",
            "aggregated_usual_environments_silver",
        ],
        "output": [
            "multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object",
            "SilverAggregatedUsualEnvironmentsZonesDataObject",
            "aggregated_usual_environments_zone_silver",
        ],
    },
}


class SpatialAggregation(Component):
    """
    This class is responsible for spatial aggregation of the gridded indicators to geographical zones of interest.
    """

    COMPONENT_ID = "SpatialAggregation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.zoning_dataset_id = self.config.get(SpatialAggregation.COMPONENT_ID, "zonning_dataset_id")
        hierarchical_levels_raw = self.config.get(SpatialAggregation.COMPONENT_ID, "hierarchical_levels").split(",")

        self.hierarchical_levels = []
        # check levels are numeric, otherwise raise error
        for level in hierarchical_levels_raw:
            try:
                self.hierarchical_levels.append(int(level))
            except ValueError:
                raise ValueError(f"Invalid hierarchical level: {level}")

        self.current_level = None

    def initalize_data_objects(self):

        # inputs
        self.clear_destination_directory = self.config.getboolean(
            SpatialAggregation.COMPONENT_ID, "clear_destination_directory"
        )

        self.aggregation_type = self.config.get(SpatialAggregation.COMPONENT_ID, "aggregation_type")

        if self.aggregation_type not in CLASS_MAPPING.keys():
            raise ValueError(f"Invalid aggregation type: {self.aggregation_type}")

        # prepare input data objects to aggregate
        input_aggregation_do_params = CLASS_MAPPING[self.aggregation_type]["input"]
        self.input_aggregation_do = self.import_class(input_aggregation_do_params[0], input_aggregation_do_params[1])
        inputs = {
            input_aggregation_do_params[2]: self.input_aggregation_do,
            "geozones_grid_map_data_silver": SilverGeozonesGridMapDataObject,
        }

        self.input_data_objects = {}
        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # prepare output data objects
        output_do_params = CLASS_MAPPING[self.aggregation_type]["output"]
        self.output_aggregation_do = self.import_class(output_do_params[0], output_do_params[1])
        output_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, output_do_params[2])

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, output_do_path)

        self.output_data_objects = {}
        self.output_data_objects[self.output_aggregation_do.ID] = self.output_aggregation_do(self.spark, output_do_path)

    @staticmethod
    def import_class(class_path: str, class_name: str):
        module = importlib.import_module(class_path)
        return getattr(module, class_name)

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        # iterate over each hierarchichal level of zoning dataset
        for level in self.hierarchical_levels:
            self.logger.info(f"Starting aggregation for level {level} ...")
            self.current_level = level
            self.read()
            self.transform()
            self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        current_zoning_sdf = self.input_data_objects[SilverGeozonesGridMapDataObject.ID].df
        current_zoning_sdf = current_zoning_sdf.filter(
            current_zoning_sdf[ColNames.dataset_id].isin(self.zoning_dataset_id)
        ).select(ColNames.grid_id, ColNames.hierarchical_id, ColNames.zone_id, ColNames.dataset_id)

        current_input_sdf = self.input_data_objects[self.input_aggregation_do.ID].df

        # do aggregation
        aggregated_sdf = self.aggregate_to_zone(
            current_input_sdf, current_zoning_sdf, self.current_level, self.output_aggregation_do
        )

        aggregated_sdf = utils.apply_schema_casting(aggregated_sdf, self.output_aggregation_do.SCHEMA)

        self.output_data_objects[self.output_aggregation_do.ID].df = aggregated_sdf

    @staticmethod
    def aggregate_to_zone(
        sdf_to_aggregate: DataFrame, zone_to_grid_map_sdf: DataFrame, hierarchy_level: int, output_do: DataObject
    ) -> DataFrame:
        """
        This method aggregates the input data to the desired hierarchical zone level

        args:
            sdf_to_aggregate: DataFrame - input data to aggregate
            zone_to_grid_map_sdf: DataFrame - mapping of grid tiles to zones
            hierarchy_level: int - desired hierarchical zone level
            output_do: DataObject - output data object

        returns:
            sdf_to_aggregate: DataFrame - aggregated data
        """
        # Override zone_id with the desired hierarchical zone level.
        zone_to_grid_map_sdf = zone_to_grid_map_sdf.withColumn(
            ColNames.zone_id,
            F.element_at(F.split(F.col(ColNames.hierarchical_id), pattern="\\|"), hierarchy_level),
        )
        zone_to_grid_map_sdf = zone_to_grid_map_sdf.withColumn(ColNames.level, F.lit(hierarchy_level))

        sdf_to_aggregate = sdf_to_aggregate.join(zone_to_grid_map_sdf, on=ColNames.grid_id)

        # potentially different aggregation functions can be used
        agg_expressions = [F.sum(F.col(col)).alias(col) for col in output_do.VALUE_COLUMNS]
        aggregated_sdf = sdf_to_aggregate.groupBy(*output_do.AGGREGATION_COLUMNS).agg(*agg_expressions)

        return aggregated_sdf
