"""
This module is responsible for aggregation of the gridded indicators to geographical zones of interest.
"""

import importlib
import datetime as dt
import calendar as cal

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from multimno.core.component import Component
from multimno.core.data_objects.data_object import DataObject
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import (
    SilverGeozonesGridMapDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_data_object import (
    SilverPresentPopulationDataObject,
)
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import (
    CONFIG_SILVER_PATHS_KEY,
)
from multimno.core.constants.columns import ColNames
from multimno.core.constants.period_names import SEASONS
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats

CLASS_MAPPING = {
    "PresentPopulation": {
        "input": [
            SilverPresentPopulationDataObject,
            "present_population_silver",
        ],
        "output": [
            SilverPresentPopulationZoneDataObject,
            "present_population_zone_silver",
        ],
    },
    "UsualEnvironment": {
        "input": [
            SilverAggregatedUsualEnvironmentsDataObject,
            "aggregated_usual_environments_silver",
        ],
        "output": [
            SilverAggregatedUsualEnvironmentsZonesDataObject,
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

        self.zoning_dataset_id = None
        self.current_level = None
        self.input_aggregation_do = None

    def initalize_data_objects(self):

        # inputs
        self.aggegation_targets = []
        if self.config.getboolean(SpatialAggregation.COMPONENT_ID, "present_population_execution"):
            self.aggegation_targets.append("PresentPopulation")

        if self.config.getboolean(SpatialAggregation.COMPONENT_ID, "usual_environment_execution"):
            self.aggegation_targets.append("UsualEnvironment")

        if len(self.aggegation_targets) == 0:
            raise ValueError("No aggregation targets specified")

        # prepare input data objects to aggregate

        inputs = {"geozones_grid_map_data_silver": SilverGeozonesGridMapDataObject}

        for target in self.aggegation_targets:
            input_aggregation_do_params = CLASS_MAPPING[target]["input"]
            inputs[input_aggregation_do_params[1]] = input_aggregation_do_params[0]

        self.input_data_objects = {}
        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # prepare output data objects

        self.output_data_objects = {}
        for target in self.aggegation_targets:
            output_do_params = CLASS_MAPPING[target]["output"]
            output_do_path = self.config.get(CONFIG_SILVER_PATHS_KEY, output_do_params[1])

            if self.config.get(f"{SpatialAggregation.COMPONENT_ID}.{target}", "clear_destination_directory"):
                delete_file_or_folder(self.spark, output_do_path)
            self.output_data_objects[output_do_params[0].ID] = output_do_params[0](self.spark, output_do_path)
            self.output_data_objects[output_do_params[0].ID].df = self.spark.createDataFrame(
                self.spark.sparkContext.emptyRDD(), output_do_params[0].SCHEMA
            )

    def filter_dataframe(self, aggregation_target: str) -> DataFrame:
        """Filtering function that takes the partitions of the dataframe specified via configuration file

        Args:
            df (DataFrame): original DataFrame

        Raises:
            ValueError: if `season` value in configuration file is not one of allowed values

        Returns:
            DataFrame: filtered DataFrame
        """

        current_input_sdf = self.input_data_objects[self.input_aggregation_do.ID].df
        if aggregation_target == "PresentPopulation":
            start_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "start_date"), "%Y-%m-%d"
            )
            end_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "end_date"), "%Y-%m-%d"
            )

            current_input_sdf = current_input_sdf.where(
                F.make_date(ColNames.year, ColNames.month, ColNames.day).between(start_date, end_date)
            )

        if aggregation_target == "UsualEnvironment":
            labels = self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "labels")
            labels = list(x.strip() for x in labels.split(","))

            start_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "start_month"), "%Y-%m"
            )
            end_date = dt.datetime.strptime(
                self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "end_month"), "%Y-%m"
            )
            end_date = end_date + dt.timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
            season = self.config.get(f"{self.COMPONENT_ID}.{aggregation_target}", "season")
            if season not in SEASONS:
                raise ValueError(f"Unknown season {season} -- valid values are {SEASONS}")

            current_input_sdf = (
                current_input_sdf.where(F.col(ColNames.label).isin(labels))
                .where(F.col(ColNames.start_date) == start_date)
                .where(F.col(ColNames.end_date) == end_date)
                .where(F.col(ColNames.season) == season)
            )

        return current_input_sdf

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        for target in self.aggegation_targets:
            self.logger.info(f"Starting spatial aggregation for {target} ...")
            self.input_aggregation_do = self.input_data_objects[CLASS_MAPPING[target]["input"][0].ID]
            self.output_aggregation_do = self.output_data_objects[CLASS_MAPPING[target]["output"][0].ID]

            self.zoning_dataset_id = self.config.get(f"{self.COMPONENT_ID}.{target}", "zoning_dataset_id")
            levels = self.config.get(f"{self.COMPONENT_ID}.{target}", "hierarchical_levels")
            levels = list(int(x.strip()) for x in levels.split(","))

            self.read()
            self.current_input_sdf = self.filter_dataframe(target)
            # iterate over each hierarchichal level of zoning dataset
            for level in levels:
                self.logger.info(f"Starting aggregation for level {level} ...")
                self.current_level = level
                self.transform()
                self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}...")

        current_zoning_sdf = self.input_data_objects[SilverGeozonesGridMapDataObject.ID].df
        current_zoning_sdf = current_zoning_sdf.filter(
            current_zoning_sdf[ColNames.dataset_id].isin(self.zoning_dataset_id)
        ).select(ColNames.grid_id, ColNames.hierarchical_id, ColNames.zone_id, ColNames.dataset_id)

        # do aggregation
        aggregated_sdf = self.aggregate_to_zone(
            self.current_input_sdf, current_zoning_sdf, self.current_level, self.output_aggregation_do
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
