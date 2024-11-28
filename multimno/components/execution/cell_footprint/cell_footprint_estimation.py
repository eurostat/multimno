"""
Module that cleans RAW MNO Event data.
"""

from math import sqrt, pi
import datetime
from typing import Union
from itertools import combinations
from functools import reduce

import pandas as pd
import numpy as np
from pyspark.sql import Row, Column, DataFrame, Window
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.storagelevel import StorageLevel
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP
from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_enriched_grid_data_object import (
    SilverEnrichedGridDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import (
    SilverCellFootprintDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
import multimno.core.utils as utils


class CellFootprintEstimation(Component):
    """
    This class is responsible for modeling the signal strength of a cellular network.

    It takes as input a configuration file and a set of data representing the network's cells and their properties.
    The class then calculates the signal strength at various points of a grid, taking into account factors such
    as the distance to the cell, the azimuth and elevation angles, and the directionality of the cell.

    The class provides methods for adjusting the signal strength based on the horizontal and vertical angles,
    imputing default cell properties.
    """

    COMPONENT_ID = "CellFootprintEstimation"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.do_azimuth_angle_adjustments = self.config.getboolean(self.COMPONENT_ID, "do_azimuth_angle_adjustments")
        self.do_elevation_angle_adjustments = self.config.getboolean(
            self.COMPONENT_ID, "do_elevation_angle_adjustments"
        )
        self.default_cell_properties = self.config.geteval(self.COMPONENT_ID, "default_cell_physical_properties")

        self.logistic_function_steepness = self.config.getfloat(self.COMPONENT_ID, "logistic_function_steepness")
        self.logistic_function_midpoint = self.config.getfloat(self.COMPONENT_ID, "logistic_function_midpoint")
        self.signal_dominance_treshold = self.config.getfloat(self.COMPONENT_ID, "signal_dominance_treshold")
        self.max_cells_per_grid_tile = self.config.getfloat(self.COMPONENT_ID, "max_cells_per_grid_tile")

        self.do_dynamic_coverage_range_calculation = self.config.getboolean(
            self.COMPONENT_ID, "do_dynamic_coverage_range_calculation"
        )
        self.repartition_number = self.config.getint(self.COMPONENT_ID, "repartition_number")
        self.coverage_range_line_buffer = self.config.getint(self.COMPONENT_ID, "coverage_range_line_buffer")

        self.difference_from_best_sd_treshold = self.config.getfloat(
            self.COMPONENT_ID, "difference_from_best_sd_treshold"
        )
        self.do_sd_treshold_prunning = self.config.getboolean(self.COMPONENT_ID, "do_sd_treshold_prunning")
        self.do_max_cells_per_tile_prunning = self.config.getboolean(
            self.COMPONENT_ID, "do_max_cells_per_tile_prunning"
        )
        self.do_difference_from_best_sd_prunning = self.config.getboolean(
            self.COMPONENT_ID, "do_difference_from_best_sd_prunning"
        )

        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        self.sd_azimuth_mapping_sdf = None
        self.sd_elevation_mapping_sdf = None
        self.current_date = None
        self.current_cells_sdf = None

        self.partition_number = self.config.getint(self.COMPONENT_ID, "partition_number")

    def initalize_data_objects(self):

        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        self.cartesian_crs = self.config.get(CellFootprintEstimation.COMPONENT_ID, "cartesian_crs")
        self.use_elevation = self.config.getboolean(CellFootprintEstimation.COMPONENT_ID, "use_elevation")

        self.input_data_objects = {}
        # Input
        inputs = {
            "network_data_silver": SilverNetworkDataObject,
        }
        if self.use_elevation:
            inputs["enriched_grid_data_silver"] = SilverEnrichedGridDataObject
        else:
            inputs["grid_data_silver"] = SilverGridDataObject

        for key, value in inputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if check_if_data_path_exists(self.spark, path):
                self.input_data_objects[value.ID] = value(self.spark, path)
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
                raise ValueError(f"Invalid path for {value.ID}: {path}")

        # Output
        self.output_data_objects = {}
        cell_footprint_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, cell_footprint_path)

        self.output_data_objects[SilverCellFootprintDataObject.ID] = SilverCellFootprintDataObject(
            self.spark, cell_footprint_path
        )

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        # for every date in the data period, get the events
        # for that date and calculate the time segments
        for current_date in self.data_period_dates:

            self.logger.info(f"Processing cell plan for {current_date.strftime('%Y-%m-%d')}")

            self.current_date = current_date

            self.current_cells_sdf = (
                self.input_data_objects[SilverNetworkDataObject.ID]
                .df.filter(
                    (
                        F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                        == F.lit(current_date)
                    )
                )
                .select(
                    ColNames.cell_id,
                    ColNames.cell_type,
                    ColNames.antenna_height,
                    ColNames.power,
                    ColNames.range,
                    ColNames.horizontal_beam_width,
                    ColNames.vertical_beam_width,
                    ColNames.altitude,
                    ColNames.latitude,
                    ColNames.longitude,
                    ColNames.directionality,
                    ColNames.azimuth_angle,
                    ColNames.elevation_angle,
                )
            )

            self.transform()
            self.write()
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        if self.use_elevation:
            grid_sdf = self.input_data_objects[SilverEnrichedGridDataObject.ID].df.select(
                ColNames.grid_id, ColNames.geometry, ColNames.elevation
            )
        else:
            grid_sdf = self.input_data_objects[SilverGridDataObject.ID].df.select(ColNames.grid_id, ColNames.geometry)

        grid_sdf = self.add_z_to_point_geometry(grid_sdf, ColNames.geometry, self.use_elevation)
        grid_sdf = grid_sdf.withColumnRenamed(ColNames.geometry, ColNames.joined_geometry)

        current_cells_sdf = self.current_cells_sdf

        current_cells_sdf = self.impute_default_cell_properties(current_cells_sdf)

        current_cells_sdf = self.watt_to_dbm(current_cells_sdf)

        # TODO: Add Path Loss Exponent calculation based on grid landuse data

        # Create geometries
        current_cells_sdf = self.create_cell_point_geometry(current_cells_sdf, self.use_elevation)
        current_cells_sdf = utils.project_to_crs(current_cells_sdf, 4326, self.cartesian_crs)

        if self.do_azimuth_angle_adjustments:
            # get standard deviation mapping table for azimuth beam width azimuth back loss pairs
            self.sd_azimuth_mapping_sdf = self.get_angular_adjustments_sd_mapping(
                current_cells_sdf,
                ColNames.horizontal_beam_width,
                ColNames.azimuth_signal_strength_back_loss,
                "azimuth",
            )

        self.sd_azimuth_mapping_sdf = F.broadcast(self.sd_azimuth_mapping_sdf)

        if self.do_elevation_angle_adjustments:
            # get standard deviation mapping table for elevation beam width elevation back loss pairs
            self.sd_elevation_mapping_sdf = self.get_angular_adjustments_sd_mapping(
                current_cells_sdf,
                ColNames.vertical_beam_width,
                ColNames.elevation_signal_strength_back_loss,
                "elevation",
            )

        self.sd_elevation_mapping_sdf = F.broadcast(self.sd_elevation_mapping_sdf)

        if self.do_dynamic_coverage_range_calculation:
            current_cells_sdf = self.calculate_effective_coverage(current_cells_sdf, grid_sdf)
            current_cells_sdf = current_cells_sdf.dropna(subset=["coverage_center"])
            current_cells_sdf = current_cells_sdf.filter(F.col("coverage_effective_range") > 100)
            current_cells_sdf = current_cells_sdf.repartition(self.repartition_number)
            current_cells_sdf.cache()
            current_cells_sdf.count()
            current_cell_grid_sdf = self.spatial_join_within_distance(
                current_cells_sdf, grid_sdf, "coverage_center", "coverage_effective_range"
            )
        else:
            current_cell_grid_sdf = self.spatial_join_within_distance(current_cells_sdf, grid_sdf, "geometry", "range")
        # Calculate planar and 3D distances
        current_cell_grid_sdf = self.calculate_cartesian_distances(current_cell_grid_sdf)

        current_cell_grid_sdf = self.calculate_signal_dominance(
            current_cell_grid_sdf, self.do_azimuth_angle_adjustments, self.do_elevation_angle_adjustments
        )

        current_cell_grid_sdf = current_cell_grid_sdf.drop(
            ColNames.distance_to_cell_3D,
            ColNames.distance_to_cell,
            ColNames.azimuth_angle,
            ColNames.elevation_angle,
            ColNames.horizontal_beam_width,
            ColNames.vertical_beam_width,
            ColNames.azimuth_signal_strength_back_loss,
            ColNames.elevation_signal_strength_back_loss,
        )

        # Prune small signal dominance values
        if self.do_sd_treshold_prunning:
            current_cell_grid_sdf = self.prune_small_signal_dominance(
                current_cell_grid_sdf, self.signal_dominance_treshold
            )

        # Prune signal dominance difference from best
        if self.do_difference_from_best_sd_prunning:
            current_cell_grid_sdf = self.prune_signal_difference_from_best(
                current_cell_grid_sdf, self.difference_from_best_sd_treshold
            )

        # Prune max cells per grid tile
        if self.do_max_cells_per_tile_prunning:
            current_cell_grid_sdf = self.prune_max_cells_per_grid_tile(
                current_cell_grid_sdf, self.max_cells_per_grid_tile
            )

        # get year, month, day from current date

        current_cell_grid_sdf = current_cell_grid_sdf.withColumns(
            {
                ColNames.year: F.lit(self.current_date.year),
                ColNames.month: F.lit(self.current_date.month),
                ColNames.day: F.lit(self.current_date.day),
            }
        )

        current_cell_grid_sdf = utils.apply_schema_casting(current_cell_grid_sdf, SilverCellFootprintDataObject.SCHEMA)

        current_cell_grid_sdf = current_cell_grid_sdf.coalesce(self.partition_number)

        self.output_data_objects[SilverCellFootprintDataObject.ID].df = current_cell_grid_sdf

    def impute_default_cell_properties(self, sdf: DataFrame) -> DataFrame:
        """
        Imputes default cell properties for null values in the input DataFrame using
        default properties for cell types from config.

        Args:
            sdf (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with imputed default cell properties.
        """
        default_properties_df = self.create_default_properties_df()

        # add default prefix to the columns of default_properties_df
        default_properties_df = default_properties_df.select(
            [F.col(col).alias(f"default_{col}") for col in default_properties_df.columns]
        )

        # assign default cell type to cell types not present in config
        sdf = sdf.withColumn(
            ColNames.cell_type,
            F.when(
                F.col(ColNames.cell_type).isin(list(self.default_cell_properties.keys())),
                F.col(ColNames.cell_type),
            ).otherwise("default"),
        )

        # all cell types which are absent from the default_properties_df will be assigned default values
        sdf = sdf.join(
            default_properties_df,
            sdf[ColNames.cell_type] == default_properties_df[f"default_{ColNames.cell_type}"],
            how="inner",
        )
        # if orignal column is null, assign the default value
        for col in default_properties_df.columns:
            col = col.replace("default_", "")
            if col not in sdf.columns:
                sdf = sdf.withColumn(col, F.lit(None))
            sdf = sdf.withColumn(col, F.coalesce(F.col(col), F.col(f"default_{col}")))

        return sdf.drop(*default_properties_df.columns)

    def create_default_properties_df(self) -> DataFrame:
        """
        Creates a DataFrame with default cell properties from config dict.

        Returns:
            DataFrame: A DataFrame with default cell properties.
        """

        rows = [Row(cell_type=k, **v) for k, v in self.default_cell_properties.items()]
        return self.spark.createDataFrame(rows).withColumnRenamed("cell_type", ColNames.cell_type)

    # create geometry for cells. Set Z values if elevation is taken into account from z column, otherwise to 0
    @staticmethod
    def create_cell_point_geometry(sdf: DataFrame, use_elevation: bool) -> DataFrame:
        """
        Creates cell point geometry.
        If elevation is taken into account, set Z values from z column, otherwise to 0.

        Args:
            sdf (DataFrame): Input DataFrame.
            use_elevation (bool): Whether to use elevation.

        Returns:
            DataFrame: DataFrame with cell point geometry.
        """
        if use_elevation:
            sdf = sdf.withColumn(
                ColNames.geometry,
                STC.ST_MakePoint(
                    F.col(ColNames.longitude),
                    F.col(ColNames.latitude),
                    F.col(ColNames.altitude) + F.col(ColNames.antenna_height),
                ),
            )
        else:
            sdf = sdf.withColumn(
                ColNames.geometry,
                STC.ST_MakePoint(
                    F.col(ColNames.longitude),
                    F.col(ColNames.latitude),
                    F.col(ColNames.antenna_height),
                ),
            )
        # assign crs
        sdf = sdf.withColumn(ColNames.geometry, STF.ST_SetSRID(F.col(ColNames.geometry), F.lit(4326)))

        return sdf.drop(ColNames.latitude, ColNames.longitude, ColNames.altitude, ColNames.antenna_height)

    # add z value to the grid geometry if elevation is taken into account from z column in the grid otherwise set to 0
    @staticmethod
    def add_z_to_point_geometry(sdf: DataFrame, geometry_col: str, use_elevation: bool) -> DataFrame:
        """
        Adds z value to the point geometry (grid centroids).
        If elevation is taken into account, set Z values from z column, otherwise to 0.

        Args:
            sdf (DataFrame): Input DataFrame.
            use_elevation (bool): Whether to use elevation.

        Returns:
            DataFrame: DataFrame with z value added to point geometry.
        """
        if use_elevation:
            sdf = sdf.withColumn(
                geometry_col,
                STC.ST_MakePoint(
                    STF.ST_X(F.col(geometry_col)),
                    STF.ST_Y(F.col(geometry_col)),
                    F.col(ColNames.elevation),
                ),
            )
        else:
            sdf = sdf.withColumn(
                geometry_col,
                STC.ST_MakePoint(
                    STF.ST_X(F.col(geometry_col)),
                    STF.ST_Y(F.col(geometry_col)),
                    F.lit(0.0),
                ),
            )

        return sdf.drop(ColNames.elevation)

    @staticmethod
    def spatial_join_within_distance(
        sdf_from: DataFrame, sdf_to: DataFrame, geometry_col: str, within_distance_col: str
    ) -> DataFrame:
        """
        Performs a spatial join within a specified distance.

        Args:
            sdf_from (DataFrame): Input DataFrame.
            sdf_to (DataFrame): DataFrame to join with.
            within_distance_col (str): Column name for the within distance.

        Returns:
            DataFrame: DataFrame after performing the spatial join.
        """

        sdf_merged = (
            sdf_from.alias("a")
            .join(
                sdf_to.alias("b"),
                STP.ST_Intersects(
                    STF.ST_Buffer(f"a.{geometry_col}", f"a.{within_distance_col}"),
                    f"b.{ColNames.joined_geometry}",
                ),
            )
            .drop(f"a.{within_distance_col}")
        )

        return sdf_merged

    @staticmethod
    def calculate_cartesian_distances(sdf: DataFrame) -> DataFrame:
        """
        Calculates cartesian distances.

        Args:
            sdf (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with calculated cartesian distances.
        """

        sdf = sdf.withColumns(
            {
                ColNames.distance_to_cell_3D: STF.ST_3DDistance(
                    F.col(ColNames.geometry), F.col(ColNames.joined_geometry)
                ),
                ColNames.distance_to_cell: STF.ST_Distance(F.col(ColNames.geometry), F.col(ColNames.joined_geometry)),
            }
        )

        return sdf

    @staticmethod
    def watt_to_dbm(sdf: DataFrame) -> DataFrame:
        """
        Converts power from watt to dBm.

        Args:
            sdf (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with power converted to dBm.
        """
        return sdf.withColumn(ColNames.power, 10.0 * F.log10(F.col(ColNames.power)) + 30.0)

    @staticmethod
    def join_sd_mapping(
        sdf: DataFrame,
        sd_mapping_sdf: DataFrame,
        beam_width_col: str,
        signal_front_back_difference_col: str,
        sd_col: str,
    ) -> DataFrame:
        """
        Joins DataFrame with standard deviation mapping.

        Args:
            sdf (DataFrame): Input DataFrame.
            sd_mapping_sdf (DataFrame): DataFrame with standard deviation mapping.
            beam_width_col (str): Column name for the beam width.
            signal_front_back_difference_col (str): Column name for the signal front-back difference.

        Returns:
            DataFrame: DataFrame after joining with standard deviation mapping.
        """

        join_condition = (F.col(f"a.{beam_width_col}") == F.col(f"b.{beam_width_col}")) & (
            F.col(f"a.{signal_front_back_difference_col}") == F.col(f"b.{signal_front_back_difference_col}")
        )

        sdf = sdf.alias("a").join(sd_mapping_sdf.alias("b"), join_condition).select(f"a.*", f"b.{sd_col}")

        return sdf

    def get_angular_adjustments_sd_mapping(
        self,
        cells_sdf: DataFrame,
        beam_width_col: str,
        signal_front_back_difference_col: str,
        angular_adjustment_type: str,
    ) -> DataFrame:
        """
        Calculates standard deviations in signal strength based on beam width and front-back cell signal difference.

        Args:
            cells_sdf (DataFrame): Input DataFrame.
            beam_width_col (str): Column name for the beam width.
            signal_front_back_difference_col (str): Column name for the signal front-back difference.
            angular_adjustment_type (str): Type of angular adjustment.

        Returns:
            DataFrame: DataFrame with angular adjustments standard deviation mapping.
        """

        sd_mappings = CellFootprintEstimation.get_sd_to_signal_back_loss_mappings(
            cells_sdf, signal_front_back_difference_col
        )
        beam_widths_diff = (
            cells_sdf.select(F.col(beam_width_col), F.col(signal_front_back_difference_col))
            .where(F.col(ColNames.directionality) == 1)
            .distinct()
        )
        beam_widths_diff = [row.asDict() for row in beam_widths_diff.collect()]

        beam_sds = []
        for item in beam_widths_diff:
            item[f"sd_{angular_adjustment_type}"] = self.find_sd(
                item[beam_width_col],
                sd_mappings[sd_mappings[signal_front_back_difference_col] == item[signal_front_back_difference_col]],
            )
            beam_sds.append(item)

        beam_sd_sdf = self.spark.createDataFrame(beam_sds)

        return beam_sd_sdf

    @staticmethod
    def find_sd(beam_width: float, mapping: DataFrame) -> float:
        """
        Finds the standard deviation corresponding to the given beam width using the provided mapping.

        Args:
            beam_width (float): The width of the beam in degrees.
            mapping (DataFrame): A DataFrame where each row corresponds to a standard deviation
                and contains the corresponding angle.

        Returns:
            float: The standard deviation corresponding to the given beam width.
        """
        min_diff_index = (abs(mapping["deg"] - beam_width / 2)).idxmin()
        return float(mapping.loc[min_diff_index, "sd"])

    @staticmethod
    def get_sd_to_signal_back_loss_mappings(cells_sdf: DataFrame, signal_front_back_difference_col: str) -> DataFrame:
        """
        Generates a DataFrame with mapping of signal strength standard deviation for each
            elevation/azimuth angle degree.

        Parameters:
        cells_sdf (DataFrame): A Spark DataFrame containing information about the cells.
        signal_front_back_difference_col (str): The name of the column that contains the difference
            in signal strength between
        the front and back of the cell.

        Returns:
        DataFrame: A pandas DataFrame with standard deviation mappings.

        """
        db_back_diffs = (
            cells_sdf.select(F.col(signal_front_back_difference_col))
            .where(F.col(ColNames.directionality) == 1)
            .distinct()
        )
        db_back_diffs = [row.asDict()[signal_front_back_difference_col] for row in db_back_diffs.collect()]
        mappings = [
            CellFootprintEstimation.create_mapping(item, signal_front_back_difference_col) for item in db_back_diffs
        ]

        return pd.concat(mappings)

    @staticmethod
    def create_mapping(db_back: float, signal_front_back_difference_col) -> DataFrame:
        """
        Creates a mapping between standard deviation and the angle
        at which the signal strength falls to 3 dB below its maximum value.

        Args:
            db_back (float): The difference in signal strength in dB between the front and back of the signal.

        Returns:
            DataFrame: A DataFrame where each row corresponds to a
            standard deviation and contains the corresponding angle.
        """
        idf = pd.DataFrame({"sd": np.arange(180 / 1000, 180, 180 / 1000)})
        idf["deg"] = idf["sd"].apply(CellFootprintEstimation.get_min3db, db_back=db_back)
        df = pd.DataFrame({"deg": np.arange(1, 181)})
        df["sd"] = df["deg"].apply(lambda dg: idf.loc[np.abs(idf["deg"] - dg).idxmin(), "sd"])
        df[signal_front_back_difference_col] = db_back
        return df

    @staticmethod
    def get_min3db(sd: float, db_back: float) -> float:
        """
        Finds the angle at which the signal strength falls to 3 dB below its maximum value.

        Args:
            sd (float): The standard deviation of the normal distribution modeling the signal strength.
            db_back (float): The difference in signal strength in dB between the front and back of the signal.

        Returns:
            float: The angle at which the signal strength falls to 3 dB below its maximum value.
        """
        df = pd.DataFrame({"a": np.linspace(0, 180, 720)})
        df["dbLoss"] = CellFootprintEstimation.norm_dBloss(df["a"], db_back=db_back, sd=sd)
        return df.loc[np.abs(-3 - df["dbLoss"]).idxmin(), "a"]

    @staticmethod
    def norm_dBloss(a: float, sd: float, db_back: float) -> float:
        """
        Computes the loss in signal strength in dB as a function of
        angle from the direction of maximum signal strength.

        Args:
            a (float): The angle from the direction of maximum signal strength.
            sd (float): The standard deviation of the normal distribution modeling the signal strength.
            db_back (float): The difference in signal strength in dB between the front and back of the signal.

        Returns:
            float: The loss in signal strength in dB at the given angle.
        """
        a = ((a + 180) % 360) - 180
        inflate = -db_back / (
            CellFootprintEstimation.normal_distribution(0, 0, sd)
            - CellFootprintEstimation.normal_distribution(180, 0, sd)
        )
        return (
            CellFootprintEstimation.normal_distribution(a, 0, sd)
            - CellFootprintEstimation.normal_distribution(0, 0, sd)
        ) * inflate

    @staticmethod
    def normal_distribution(x: float, mean: float, sd: float) -> Union[np.array, list]:
        """
        Computes the value of the normal distribution with the given mean
        and standard deviation at the given point.

        Args:
            x (float): The point at which to evaluate the normal distribution.
            mean (float): The mean of the normal distribution.
            sd (float): The standard deviation of the normal distribution.
            return_type (str): The desired return type, either 'np_array' or 'list'.

        Returns:
            np.array or list: The value of the normal distribution at the given point,
            returned as either a numpy array or a list.
        """
        n_dist = (1.0 / (sqrt(2.0 * pi) * sd)) * np.exp(-0.5 * ((x - mean) / sd) ** 2)

        return n_dist

    def calculate_effective_coverage(self, cells_sdf: DataFrame, grid_sdf: DataFrame) -> DataFrame:
        """
        Calculates effective cell coverage center and range based on desired signal domincance threshold.

        The function first separates the cells into omnidirectional and directional types,
        then calculates the signal dominance threshold points for each type.
        The function then calculates the effective coverage center and range for each cell
        based on the signal dominance threshold points.

        Parameters:
        cells_sdf (pyspark.sql.DataFrame): A Spark DataFrame containing cell information, including directionality.
        grid_sdf (pyspark.sql.DataFrame): A Spark DataFrame containing grid used for calculating signal dominance.

        Returns:
        pyspark.sql.DataFrame: A Spark DataFrame with the effective coverage information for each cell, including the
                            coverage center and effective range. The DataFrame excludes intermediate columns used
                            during the calculation.
        """
        # omnidirectional cells
        cells_omni_sdf = cells_sdf.filter(F.col(ColNames.directionality) == 0)

        if cells_omni_sdf.rdd.isEmpty():
            cells_omni_sdf = self.spark.createDataFrame([], cells_sdf.schema)
        else:
            cells_omni_sdf = self.get_signal_dominance_threshold_point(cells_omni_sdf, grid_sdf, "omni")

            cells_omni_sdf = cells_omni_sdf.withColumn("coverage_center", F.col("geometry")).withColumn(
                "coverage_effective_range", STF.ST_Distance(F.col("coverage_center"), F.col("omni"))
            )
        cells_omni_sdf.cache()
        cells_omni_sdf.count()

        # directional cells
        cells_directional_sdf = cells_sdf.filter(F.col(ColNames.directionality) == 1)

        if cells_directional_sdf.rdd.isEmpty():
            cells_directional_sdf = self.spark.createDataFrame([], cells_sdf.schema)
        else:
            cells_directional_sdf = self.get_signal_dominance_threshold_point(
                cells_directional_sdf, grid_sdf, "directional_front"
            )

            cells_directional_sdf.cache()
            cells_directional_sdf.count()

            cells_directional_sdf = self.get_signal_dominance_threshold_point(
                cells_directional_sdf, grid_sdf, "directional_back"
            )

            cells_directional_sdf = cells_directional_sdf.withColumn(
                "coverage_center",
                STF.ST_LineInterpolatePoint(
                    STF.ST_MakeLine(
                        cells_directional_sdf["directional_front"], cells_directional_sdf["directional_back"]
                    ),
                    0.5,
                ),
            )
            cells_directional_sdf = cells_directional_sdf.withColumn(
                "coverage_effective_range",
                STF.ST_Distance(cells_directional_sdf["coverage_center"], cells_directional_sdf["directional_front"]),
            )

        cells_sdf = cells_omni_sdf.unionByName(cells_directional_sdf, allowMissingColumns=True)

        return cells_sdf.drop("omni", "directional_front", "directional_back")

    def get_signal_dominance_threshold_point(self, cells_sdf, grid_sdf, point_type):
        """
        Calculates the signal dominance threshold points in cell maximum range.

        For omnidirectional cell types, the signal dominance threshold point is calculated as
        the furthest point along 90 degrees azimuth direction where signal dominance is less than the threshold.
        For directional cells types, two signal dominance threshold points are calculated:
            1. the furthest point along the directionality angle direction where signal dominance is less than the threshold
            2. the furthest point opposite to the directionality angle direction where signal dominance is less than the threshold

        Args:
            cells_sdf (DataFrame): The DataFrame containing cell information.
            grid_sdf (DataFrame): The DataFrame containing grid information.
            point_type (str): The type of point to calculate the signal dominance threshold for.

        Returns:
            DataFrame: The updated cells DataFrame with the signal dominance threshold point added.
        """

        do_azimuth_angle_adjustments = True
        do_elevation_angle_adjustments = True

        if point_type == "directional_front":
            # Calculate range line for directional front point type
            cells_sdf = cells_sdf.withColumn(
                "range_line",
                STF.ST_MakeLine(
                    "geometry",
                    STC.ST_Point(
                        STF.ST_X("geometry") + cells_sdf["range"] * F.sin(F.radians(cells_sdf["azimuth_angle"])),
                        STF.ST_Y("geometry") + cells_sdf["range"] * F.cos(F.radians(cells_sdf["azimuth_angle"])),
                    ),
                ),
            )

        elif point_type == "directional_back":
            # Calculate range line for directional back point type
            cells_sdf = cells_sdf.withColumn(
                "range_line",
                STF.ST_MakeLine(
                    "geometry",
                    STC.ST_Point(
                        STF.ST_X("geometry") + cells_sdf["range"] * F.sin(F.radians(cells_sdf["azimuth_angle"] + 180)),
                        STF.ST_Y("geometry") + cells_sdf["range"] * F.cos(F.radians(cells_sdf["azimuth_angle"] + 180)),
                    ),
                ),
            )

        elif point_type == "omni":
            # Calculate range line for omni point type
            cells_sdf = cells_sdf.withColumn(
                "range_line",
                STF.ST_MakeLine(
                    "geometry",
                    STC.ST_Point(
                        STF.ST_X("geometry") + cells_sdf["range"] * F.sin(F.radians(F.lit(90))),
                        STF.ST_Y("geometry") + cells_sdf["range"] * F.cos(F.radians(F.lit(90))),
                    ),
                ),
            )

            do_azimuth_angle_adjustments = False
            do_elevation_angle_adjustments = False

        cells_sdf = cells_sdf.withColumn("coverage_range_line_buffer", F.lit(self.coverage_range_line_buffer))

        cell_grid = self.spatial_join_within_distance(cells_sdf, grid_sdf, "range_line", "coverage_range_line_buffer")

        # calculate 3d distance and planar distance from cell id to grid ids
        cell_grid = self.calculate_cartesian_distances(cell_grid)

        cell_grid = self.calculate_signal_dominance(
            cell_grid, do_elevation_angle_adjustments, do_azimuth_angle_adjustments
        )

        cell_grid_filtered = cell_grid.filter(F.col(ColNames.signal_dominance) > self.signal_dominance_treshold)
        cell_counts = cell_grid_filtered.groupBy("cell_id").count().withColumnRenamed("count", "filtered_count")
        cell_grid_joined = cell_grid.join(cell_counts, on="cell_id", how="left").fillna(0, subset=["filtered_count"])

        # In case if desired threshold filter removes all grid tiles in given direction, keep closest grid tile
        # Otherwise keep furthest point where threshold condition meet
        window_min = Window.partitionBy("cell_id").orderBy(F.col("distance_to_cell"))
        window_max = Window.partitionBy("cell_id").orderBy(F.desc(F.col("distance_to_cell")))

        cell_grid_max = (
            cell_grid_joined.withColumn(
                "rank",
                F.when(F.col("filtered_count") == 0, F.row_number().over(window_min)).otherwise(
                    F.row_number().over(window_max)
                ),
            )
            .filter(F.col("rank") == 1)
            .drop("rank", "filtered_count")
        )

        cells_sdf = cells_sdf.join(
            cell_grid_max.select("cell_id", "joined_geometry").withColumnRenamed("joined_geometry", point_type),
            "cell_id",
            "left",
        )

        return cells_sdf.drop(
            "range_line", "distance_to_cell_3d", "distance_to_cell", "signal_strength", "signal_dominance", "grid_id"
        )

    @staticmethod
    def calculate_distance_power_loss(sdf: DataFrame) -> DataFrame:
        """
        Calculates distance power loss caluclated as
        power - path_loss_exponent * 10 * log10(distance_to_cell_3D).

        Args:
            sdf (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with calculated distance power loss.
        """
        sdf = sdf.withColumn(
            ColNames.signal_strength,
            F.col(ColNames.power)
            - F.col(ColNames.path_loss_exponent) * 10 * F.log10(F.col(ColNames.distance_to_cell_3D)),
        )

        return sdf.drop(ColNames.power, ColNames.path_loss_exponent)

    def calculate_signal_dominance(self, cell_grid_gdf, do_elevation_angle_adjustments, do_azimuth_angle_adjustments):
        """
        Calculates the signal dominance for each cell in a grid DataFrame, optionally adjusting for elevation and azimuth angles.

        This function performs a series of adjustments on the signal strength of each cell in the provided grid DataFrame to calculate the signal dominance. The adjustments include distance power loss, horizontal angle power adjustment (azimuth), and vertical angle power adjustment (elevation), based on the provided flags. Finally, it converts the adjusted signal strength into signal dominance using a logistic function.

        Parameters:
            cell_grid_gdf (GeoDataFrame): A GeoDataFrame containing the cell grid data.
            do_elevation_angle_adjustments (bool): Flag to indicate whether to perform elevation angle adjustments.
            do_azimuth_angle_adjustments (bool): Flag to indicate whether to perform azimuth angle adjustments.

        Returns:
            GeoDataFrame: The input GeoDataFrame with an additional column for signal dominance, after applying the specified adjustments.
        """

        # calculate distance power loss
        cell_grid_gdf = self.calculate_distance_power_loss(cell_grid_gdf)

        # calculate horizontal angle power adjustment
        if do_azimuth_angle_adjustments:

            cell_grid_gdf_directional = cell_grid_gdf.filter(F.col(ColNames.directionality) == 1)

            cell_grid_gdf_directional = self.join_sd_mapping(
                cell_grid_gdf_directional,
                self.sd_azimuth_mapping_sdf,
                ColNames.horizontal_beam_width,
                ColNames.azimuth_signal_strength_back_loss,
                "sd_azimuth",
            )
            cell_grid_gdf_directional = self.calculate_horizontal_angle_power_adjustment(cell_grid_gdf_directional)

            cell_grid_gdf = cell_grid_gdf_directional.unionByName(
                cell_grid_gdf.where(F.col(ColNames.directionality) == 0),
                allowMissingColumns=True,
            )

        # calculate vertical angle power adjustment
        if do_elevation_angle_adjustments:

            cell_grid_gdf_directional = cell_grid_gdf.filter(F.col(ColNames.directionality) == 1)

            cell_grid_gdf_directional = self.join_sd_mapping(
                cell_grid_gdf_directional,
                self.sd_elevation_mapping_sdf,
                ColNames.vertical_beam_width,
                ColNames.elevation_signal_strength_back_loss,
                "sd_elevation",
            )

            cell_grid_gdf_directional = self.calculate_vertical_angle_power_adjustment(cell_grid_gdf_directional)
            cell_grid_gdf = cell_grid_gdf_directional.unionByName(
                cell_grid_gdf.where(F.col(ColNames.directionality) == 0),
                allowMissingColumns=True,
            )

        # calculate signal dominance
        cell_grid_gdf = self.signal_strength_to_signal_dominance(
            cell_grid_gdf, self.logistic_function_steepness, self.logistic_function_midpoint
        )

        return cell_grid_gdf

    @staticmethod
    def calculate_horizontal_angle_power_adjustment(sdf: DataFrame) -> DataFrame:
        """
        Adjusts the signal strength of each cell in the input DataFrame based on the horizontal angle.

        This function calculates the azimuth angle between each cell and a reference point,
        projects the data to the elevation plane, and adjusts the signal strength based on the
        relative azimuth angle and the distance to the cell. The adjustment is calculated using
        a normal distribution model of signal strength.

        Based on https://github.com/MobilePhoneESSnetBigData/mobloc/blob/master/R/signal_strength.R

        Args:
            sdf (DataFrame): A Spark DataFrame

        Returns:
            DataFrame: The input DataFrame with the 'signal_strength' column adjusted and intermediate columns dropped.
        """

        # TODO: simplify math in this function by using Sedona built in spatial methods
        sdf = sdf.withColumn(
            "theta_azim",
            (
                90
                - F.degrees(
                    (
                        F.atan2(
                            STF.ST_Y(F.col(ColNames.joined_geometry)) - STF.ST_Y(F.col(ColNames.geometry)),
                            STF.ST_X(F.col(ColNames.joined_geometry)) - STF.ST_X(F.col(ColNames.geometry)),
                        )
                    )
                )
            ),
        )
        sdf = sdf.withColumn(
            "theta_azim",
            F.when(F.col("theta_azim") < 0, F.col("theta_azim") + 360).otherwise(F.col("theta_azim")),
        )
        sdf = sdf.withColumn("azim", (F.col("theta_azim") - F.col(ColNames.azimuth_angle)) % 360)
        sdf = sdf.withColumn(
            "azim",
            F.when(F.col("azim") > 180, F.col("azim") - 360).otherwise(
                F.when(F.col("azim") < -180, F.col("azim") + 360).otherwise(F.col("azim"))
            ),
        )
        sdf = sdf.withColumn("a", F.sin(F.radians(F.col("azim"))) * F.col(ColNames.distance_to_cell))

        # project to elevation plane
        sdf = sdf.withColumn("b", F.cos(F.radians(F.col("azim"))) * F.col(ColNames.distance_to_cell))
        sdf = sdf.withColumn("c", STF.ST_Z(ColNames.geometry) - STF.ST_Z(ColNames.joined_geometry))

        sdf = sdf.withColumn("d", F.sqrt(F.col("b") ** 2 + F.col("c") ** 2))
        sdf = sdf.withColumn("lambda", F.degrees(F.atan2(F.col("c"), F.abs(F.col("b")))))
        sdf = sdf.withColumn(
            "cases",
            F.when(
                F.col("b") > 0,
                F.when(F.col(ColNames.elevation_angle) < F.col("lambda"), F.lit(1)).otherwise(F.lit(2)),
            ).otherwise(F.when(F.col("lambda") + F.col(ColNames.elevation_angle) < 90, F.lit(3)).otherwise(F.lit(4))),
        )

        sdf = sdf.withColumn(
            "e",
            F.when(
                F.col("cases") == 1,
                F.cos(F.radians(F.col("lambda") - F.col(ColNames.elevation_angle))) * F.col("d"),
            )
            .when(
                F.col("cases") == 2,
                F.cos(F.radians(F.col(ColNames.elevation_angle) - F.col("lambda"))) * F.col("d"),
            )
            .when(
                F.col("cases") == 3,
                -F.cos(F.radians(F.col("lambda") + F.col(ColNames.elevation_angle))) * F.col("d"),
            )
            .otherwise(F.cos(F.radians(180 - F.col("lambda") - F.col(ColNames.elevation_angle))) * F.col("d")),
        )

        sdf = sdf.withColumn("azim2", F.degrees(F.atan2(F.col("a"), F.col("e"))))

        # finally get power adjustments
        sdf = CellFootprintEstimation.norm_dBloss_spark(
            sdf, "azim2", "sd_azimuth", ColNames.azimuth_signal_strength_back_loss
        )

        sdf = sdf.withColumn("signal_strength", F.col("signal_strength") + F.col("normalized_dBloss"))

        # cleanup
        sdf = sdf.drop(
            "theta_azim",
            "azim",
            "a",
            "b",
            "c",
            "d",
            "_lambda",
            "cases",
            "e",
            "azim2",
            "sd_azimuth",
            "normalized_dBloss",
        )

        return sdf

    @staticmethod
    def calculate_vertical_angle_power_adjustment(sdf: DataFrame) -> DataFrame:
        """
        Adjusts the signal strength of each cell in the input DataFrame based on the vertical angle.

        This function calculates the elevation angle between each cell and a reference point,
        and adjusts the signal strength based on the relative elevation angle and the distance to the cell.
        The adjustment is calculated using a normal distribution model of signal strength.

        Based on https://github.com/MobilePhoneESSnetBigData/mobloc/blob/master/R/signal_strength.R

        Args:
            sdf (DataFrame): A Spark DataFrame

        Returns:
            DataFrame: The input DataFrame with the 'signal_strength' column adjusted and intermediate columns dropped.
        """

        sdf = sdf.withColumn(
            "gamma_elev",
            F.degrees(
                F.atan2(
                    STF.ST_Z(ColNames.geometry) - STF.ST_Z(ColNames.joined_geometry),
                    F.col(ColNames.distance_to_cell),
                )
            ),
        )
        sdf = sdf.withColumn("elev", (F.col("gamma_elev") - F.col(ColNames.elevation_angle)) % 360)

        sdf = sdf.withColumn(
            "elev",
            F.when(F.col("elev") > 180, F.col("elev") - 360).otherwise(
                F.when(F.col("elev") < -180, F.col("elev") + 360).otherwise(F.col("elev"))
            ),
        )

        # finally get power adjustments
        sdf = CellFootprintEstimation.norm_dBloss_spark(
            sdf, "elev", "sd_elevation", ColNames.elevation_signal_strength_back_loss
        )

        sdf = sdf.withColumn(
            ColNames.signal_strength,
            F.col(
                ColNames.signal_strength,
            )
            + F.col("normalized_dBloss"),
        )

        # cleanup
        sdf = sdf.drop("gamma_elev", "elev", "sd_elevation", "normalized_dBloss")

        return sdf

    @staticmethod
    def normal_distribution_col(x_col: Column, mean_col: Column, sd_col: Column) -> Column:
        """
        Computes the value of the normal distribution for each row in a DataFrame based on
        the provided columns for the point, mean, and standard deviation.

        This function applies the normal distribution formula to each row of the DataFrame using
        the specified columns for the point (x), mean, and standard deviation (sd).
        The normal distribution formula used is:

            f(x) = (1 / (sqrt(2 * pi) * sd)) * exp(-0.5 * ((x - mean) / sd)^2)

        where `x` is the value at which the normal distribution is evaluated,
        `mean` is the mean of the distribution, and `sd` is the standard deviation.

        Parameters:
            x_col (Column): A Spark DataFrame column representing the point at which to evaluate the normal distribution.
            mean_col (Column): A Spark DataFrame column representing the mean of the normal distribution.
            sd_col (Column): A Spark DataFrame column representing the standard deviation of the normal distribution.

        Returns:
            Column: A Spark DataFrame column with the computed normal distribution values for each row.
        """
        return (1.0 / (F.sqrt(2.0 * F.lit(pi)) * sd_col)) * F.exp(-0.5 * ((x_col - mean_col) / sd_col) ** 2)

    @staticmethod
    def norm_dBloss_spark(sdf: DataFrame, angle_col: str, sd_col: str, db_back_col: str) -> DataFrame:
        """
        Normalizes the dB loss in a Spark DataFrame based on the angle, standard deviation, and dB back loss.

        This method performs several operations to normalize the dB loss for each row in the Spark DataFrame:
        1. Normalizes the angle to a range of [-180, 180) degrees.
        2. Calculates the normal distribution of the normalized angles with a mean of 0 and a standard deviation specified by `sd_col`.
        3. Calculates the normal distribution for angles 0 and 180 degrees using precomputed values, with a mean of 0 and the same standard deviation.
        4. Computes an inflation factor based on the dB back loss column and the difference between the normal distributions at 0 and 180 degrees.
        5. Calculates the normalized dB loss by adjusting the normal distribution of the angle with the inflation factor.

        Parameters:
            sdf (DataFrame): The input Spark DataFrame containing the data.
            angle_col (str): The name of the column that contains the angles to be normalized.
            sd_col (str): The name of the column that contains the standard deviation values for the normal distribution calculation.
            db_back_col (str): The name of the column in that contains the dB back loss values used to calculate the inflation factor.

        Returns:
            DataFrame: A Spark DataFrame with the normalized dB loss added and intermediate columns removed.
        """
        # Normalizing angles

        sdf = sdf.withColumn("angle_normalized", ((F.col(angle_col) + 180) % 360) - 180)

        # Calculate the normal distribution for the normalized angles
        sdf = sdf.withColumn(
            "norm_dist_angle",
            CellFootprintEstimation.normal_distribution_col(F.col("angle_normalized"), F.lit(0), F.col(sd_col)),
        )

        # Calculate the normal distribution for 0 and 180 degrees using precomputed values
        n_dist_0 = CellFootprintEstimation.normal_distribution_col(F.lit(0), F.lit(0), F.col(sd_col))
        n_dist_180 = CellFootprintEstimation.normal_distribution_col(F.lit(180), F.lit(0), F.col(sd_col))

        # Calculate the inflated factor
        sdf = sdf.withColumn("inflate", -F.col(db_back_col) / (n_dist_0 - n_dist_180))

        # Calculate the normalized dB loss
        sdf = sdf.withColumn("normalized_dBloss", (F.col("norm_dist_angle") - n_dist_0) * F.col("inflate"))

        return sdf.drop("angle_normalized", "norm_dist_angle", "inflate")

    @staticmethod
    def signal_strength_to_signal_dominance(
        sdf: DataFrame,
        logistic_function_steepness: float,
        logistic_function_midpoint: float,
    ) -> DataFrame:
        """
        Converts signal strength to signal dominance using a logistic function.
        Methodology from A Bayesian approach to location estimation of mobile devices
        from mobile network operator data. Tennekes and Gootzen (2022).

        The logistic function is defined as 1 / (1 + exp(-scale)),
        where scale is (signal_strength - logistic_function_midpoint) * logistic_function_steepness.

        Parameters:
        sdf (DataFrame): SignalStrenghtDataObject Spark DataFrame.
        logistic_function_steepness (float): The steepness parameter for the logistic function.
        logistic_function_midpoint (float): The midpoint parameter for the logistic function.

        Returns:
        DataFrame: A Spark DataFrame with the signal dominance added as a new column.
        """
        sdf = sdf.withColumn(
            "scale",
            (F.col(ColNames.signal_strength) - logistic_function_midpoint) * logistic_function_steepness,
        )
        sdf = sdf.withColumn(ColNames.signal_dominance, 1 / (1 + F.exp(-F.col("scale"))))
        sdf = sdf.drop("scale")

        return sdf

    @staticmethod
    def prune_small_signal_dominance(sdf: DataFrame, signal_dominance_threshold: float) -> DataFrame:
        """
        Prunes rows from the DataFrame where the signal dominance is less than or equal to the provided threshold.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        signal_dominance_threshold (float): The threshold for pruning small signal dominance values.

        Returns:
        DataFrame: A DataFrame with rows having signal dominance less than or equal to the threshold removed.
        """
        sdf = sdf.filter(F.col(ColNames.signal_dominance) > signal_dominance_threshold)
        return sdf

    @staticmethod
    def prune_max_cells_per_grid_tile(sdf: DataFrame, max_cells_per_grid_tile: int) -> DataFrame:
        """
        Prunes rows from the DataFrame exceeding the maximum number of cells allowed per grid tile.

        The rows are ordered by signal dominance in descending order,
        and only the top 'max_cells_per_grid_tile' rows are kept for each grid tile.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        max_cells_per_grid_tile (int): The maximum number of cells allowed per grid tile.

        Returns:
        DataFrame: A DataFrame with rows exceeding the maximum number of cells per grid tile removed.
        """
        window = Window.partitionBy(ColNames.grid_id).orderBy(F.desc(ColNames.signal_dominance))

        sdf = sdf.withColumn("row_number", F.row_number().over(window))
        sdf = sdf.filter(F.col("row_number") <= max_cells_per_grid_tile)
        sdf = sdf.drop("row_number")

        return sdf

    @staticmethod
    def prune_signal_difference_from_best(sdf: DataFrame, difference_threshold: float) -> DataFrame:
        """
        Prunes rows from the DataFrame based on a threshold of signal dominance difference.

        The rows are ordered by signal dominance in descending order, and only the rows where the difference
        in signal dominance from the maximum is less than the threshold are kept for each grid tile.

        Parameters:
        sdf (DataFrame): A Spark DataFrame containing the signal dominance data.
        threshold (float): The threshold for signal dominance difference in percentage.

        Returns:
        DataFrame: A DataFrame with rows pruned based on the signal dominance difference threshold.
        """
        window = Window.partitionBy(ColNames.grid_id).orderBy(F.desc(ColNames.signal_dominance))

        sdf = sdf.withColumn("row_number", F.row_number().over(window))

        sdf = sdf.withColumn("max_signal_dominance", F.first(ColNames.signal_dominance).over(window))
        sdf = sdf.withColumn(
            "signal_dominance_diff_percentage",
            (sdf["max_signal_dominance"] - sdf[ColNames.signal_dominance]) / sdf["max_signal_dominance"] * 100,
        )

        sdf = sdf.filter(
            (F.col("row_number") == 1) | (F.col("signal_dominance_diff_percentage") <= difference_threshold)
        )

        sdf = sdf.drop("row_number", "max_signal_dominance", "signal_dominance_diff_percentage")

        return sdf
