"""
Module that cleans RAW MNO Event data.
"""

from math import sqrt, pi
import datetime
from typing import Union

import pandas as pd
import numpy as np
from pyspark.sql import Row, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP
from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)
from multimno.core.data_objects.silver.silver_signal_strength_data_object import (
    SilverSignalStrengthDataObject,
)
from multimno.core.spark_session import check_if_data_path_exists
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames


class SignalStrengthModeling(Component):
    """
    This class is responsible for modeling the signal strength of a cellular network.

    It takes as input a configuration file and a set of data representing the network's cells and their properties.
    The class then calculates the signal strength at various points of a grid, taking into account factors such
    as the distance to the cell, the azimuth and elevation angles, and the directionality of the cell.

    The class provides methods for adjusting the signal strength based on the horizontal and vertical angles,
    imputing default cell properties.
    """

    COMPONENT_ID = "SignalStrengthModeling"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.data_period_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()
        self.use_elevation = self.config.getboolean(SignalStrengthModeling.COMPONENT_ID, "use_elevation")
        self.do_azimuth_angle_adjustments = self.config.getboolean(
            SignalStrengthModeling.COMPONENT_ID, "do_azimuth_angle_adjustments"
        )
        self.do_elevation_angle_adjustments = self.config.getboolean(
            SignalStrengthModeling.COMPONENT_ID, "do_elevation_angle_adjustments"
        )
        self.default_cell_properties = self.config.geteval(
            SignalStrengthModeling.COMPONENT_ID, "default_cell_physical_properties"
        )

        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

    def initalize_data_objects(self):

        # Input
        self.input_data_objects = {}
        self.cartesian_crs = self.config.get(SignalStrengthModeling.COMPONENT_ID, "cartesian_crs")

        grid_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "grid_data_silver")
        if check_if_data_path_exists(self.spark, grid_path):
            self.input_data_objects[SilverGridDataObject.ID] = SilverGridDataObject(
                self.spark, grid_path, default_crs=self.cartesian_crs
            )
        else:
            self.logger.warning(f"Expected path {grid_path} to exist but it does not")
            raise ValueError(f"Invalid path for {grid_path} {SignalStrengthModeling.COMPONENT_ID}")

        network_topology_cells_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        if check_if_data_path_exists(self.spark, network_topology_cells_path):
            self.input_data_objects[SilverNetworkDataObject.ID] = SilverNetworkDataObject(
                self.spark, network_topology_cells_path
            )
        else:
            self.logger.warning(f"Expected path {network_topology_cells_path} to exist but it does not")
            raise ValueError(f"Invalid path for {network_topology_cells_path} {SignalStrengthModeling.COMPONENT_ID}")

        # Output
        self.output_data_objects = {}
        self.silver_signal_strength_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "signal_strength_data_silver")
        self.output_data_objects[SilverSignalStrengthDataObject.ID] = SilverSignalStrengthDataObject(
            self.spark,
            self.silver_signal_strength_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        grid_sdf = self.input_data_objects[SilverGridDataObject.ID].df
        grid_sdf = self.add_z_to_point_geometry(grid_sdf, ColNames.geometry, self.use_elevation)

        # TODO: We might need to iterate over dates and process the data for each date separately
        # have to do tests with larger datasets.

        # TODO: Potentially
        current_cells_sdf = self.input_data_objects[SilverNetworkDataObject.ID].df.filter(
            F.make_date("year", "month", "day").isin(self.data_period_dates)
        )

        current_cells_sdf = self.impute_default_cell_properties(current_cells_sdf)

        current_cells_sdf = self.watt_to_dbm(current_cells_sdf)

        # TODO: Add Path Loss Exponent calculation based on grid landuse data

        # Create geometries
        current_cells_sdf = self.create_cell_point_geometry(current_cells_sdf, self.use_elevation)
        current_cells_sdf = self.project_to_crs(current_cells_sdf, 4326, self.cartesian_crs)

        # Spatial join
        current_cell_grid_sdf = self.spatial_join_within_distance(current_cells_sdf, grid_sdf, ColNames.range)

        # Calculate planar and 3D distances
        current_cell_grid_sdf = self.calculate_cartesian_distances(current_cell_grid_sdf)

        # Calculate initial signal strength without azimuth and tilt angles adjustments
        current_cell_grid_sdf = self.calculate_distance_power_loss(current_cell_grid_sdf)

        # next step is to calculate azimuth and tilt angles adjustments, if set in the configuration file
        # otherwise all cells are assumed to be omnideirectional and there is no need for these adjustments

        # first need to find all standard deviations in signal strength distributions for azimuth and elevation
        # angles for each signal strength back loss. This is done only once for each unique combination of
        # signal strength back loss
        # and beam width in cells dataset

        if self.do_azimuth_angle_adjustments:

            # get standard deviation mapping table for azimuth beam width azimuth back loss pairs
            sd_azimuth_mapping_sdf = self.get_angular_adjustments_sd_mapping(
                current_cells_sdf,
                ColNames.horizontal_beam_width,
                ColNames.azimuth_signal_strength_back_loss,
                "azimuth",
            )

            current_cell_grid_directional = current_cell_grid_sdf.where(F.col(ColNames.directionality) == 1)

            current_cell_grid_directional = self.join_sd_mapping(
                current_cell_grid_directional,
                sd_azimuth_mapping_sdf,
                ColNames.horizontal_beam_width,
                ColNames.azimuth_signal_strength_back_loss,
            )

            current_cell_grid_directional = self.calculate_horizontal_angle_power_adjustment(
                current_cell_grid_directional
            )

            current_cell_grid_sdf = current_cell_grid_directional.unionByName(
                current_cell_grid_sdf.where(F.col(ColNames.directionality) == 0),
                allowMissingColumns=True,
            )

        if self.do_elevation_angle_adjustments:
            # get standard deviation mapping table for elevation beam width elevation back loss pairs
            sd_elevation_mapping_sdf = self.get_angular_adjustments_sd_mapping(
                current_cells_sdf,
                ColNames.vertical_beam_width,
                ColNames.elevation_signal_strength_back_loss,
                "elevation",
            )
            current_cell_grid_directional = current_cell_grid_sdf.where(F.col(ColNames.directionality) == 1)

            current_cell_grid_directional = self.join_sd_mapping(
                current_cell_grid_directional,
                sd_elevation_mapping_sdf,
                ColNames.vertical_beam_width,
                ColNames.elevation_signal_strength_back_loss,
            )

            current_cell_grid_directional = self.calculate_vertical_angle_power_adjustment(
                current_cell_grid_directional
            )

            current_cell_grid_sdf = current_cell_grid_directional.unionByName(
                current_cell_grid_sdf.where(F.col(ColNames.directionality) == 0),
                allowMissingColumns=True,
            )

        # amend start and end valid dates
        current_cell_grid_sdf = current_cell_grid_sdf.withColumn(
            "valid_date_start", F.make_date(F.col("year"), F.col("month"), F.col("day"))
        )
        current_cell_grid_sdf = current_cell_grid_sdf.withColumn(
            "valid_date_end", F.date_add(F.col("valid_date_start"), 1)
        )

        current_cell_grid_sdf = current_cell_grid_sdf.select(
            SilverSignalStrengthDataObject.MANDATORY_COLUMNS + SilverSignalStrengthDataObject.OPTIONAL_COLUMNS
        )
        columns = {
            field.name: F.col(field.name).cast(field.dataType) for field in SilverSignalStrengthDataObject.SCHEMA.fields
        }
        current_cell_grid_sdf = current_cell_grid_sdf.withColumns(columns)

        self.output_data_objects[SilverSignalStrengthDataObject.ID].df = current_cell_grid_sdf

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
                ~F.col(ColNames.cell_type).isin(list(self.default_cell_properties.keys())),
                "default",
            ).otherwise(F.col(ColNames.cell_type)),
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

        return sdf

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

        return sdf

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
        return sdf

    # project geometry to cartesian crs
    @staticmethod
    def project_to_crs(sdf: DataFrame, crs_in: int, crs_out: int) -> DataFrame:
        """
        Projects geometry to cartesian CRS.

        Args:
            sdf (DataFrame): Input DataFrame.
            crs_in (int): Input CRS.
            crs_out (int): Output CRS.

        Returns:
            DataFrame: DataFrame with geometry projected to cartesian CRS.
        """
        crs_in = f"EPSG:{crs_in}"
        crs_out = f"EPSG:{crs_out}"

        sdf = sdf.withColumn(
            ColNames.geometry,
            STF.ST_Transform(sdf[ColNames.geometry], F.lit(crs_in), F.lit(crs_out)),
        )
        return sdf

    @staticmethod
    def spatial_join_within_distance(sdf_from: DataFrame, sdf_to: DataFrame, within_distance_col: str) -> DataFrame:
        """
        Performs a spatial join within a specified distance.

        Args:
            sdf_from (DataFrame): Input DataFrame.
            sdf_to (DataFrame): DataFrame to join with.
            within_distance_col (str): Column name for the within distance.

        Returns:
            DataFrame: DataFrame after performing the spatial join.
        """
        sdf_to = sdf_to.withColumnRenamed(ColNames.geometry, ColNames.joined_geometry)

        sdf_merged = sdf_from.join(
            sdf_to,
            STP.ST_Intersects(
                STF.ST_Buffer(sdf_from[ColNames.geometry], sdf_from[within_distance_col]),
                sdf_to[ColNames.joined_geometry],
            ),
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
    def calculate_distance_power_loss(sdf: DataFrame) -> DataFrame:
        """
        Calculates distance power loss caluclated as
        power - path_loss_exponent * 10 * log10(distance_to_cell_3D).

        Args:
            sdf (DataFrame): Input DataFrame.

        Returns:
            DataFrame: DataFrame with calculated distance power loss.
        """
        return sdf.withColumn(
            ColNames.signal_strength,
            F.col(ColNames.power)
            - F.col(ColNames.path_loss_exponent) * 10 * F.log10(F.col(ColNames.distance_to_cell_3D)),
        )

    @staticmethod
    def join_sd_mapping(
        sdf: DataFrame,
        sd_mapping_sdf: DataFrame,
        beam_width_col: str,
        signal_front_back_difference_col: str,
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

        join_condition = (sdf[beam_width_col] == sd_mapping_sdf[beam_width_col]) & (
            sdf[signal_front_back_difference_col] == sd_mapping_sdf[signal_front_back_difference_col]
        )

        sdf = sdf.join(sd_mapping_sdf, join_condition).drop(
            sd_mapping_sdf[beam_width_col],
            sd_mapping_sdf[signal_front_back_difference_col],
        )

        return sdf

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
            SignalStrengthModeling.create_mapping(item, signal_front_back_difference_col) for item in db_back_diffs
        ]

        return pd.concat(mappings)

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

        sd_mappings = SignalStrengthModeling.get_sd_to_signal_back_loss_mappings(
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
    def normalize_angle(a: float) -> float:
        """
        Adjusts the given angle to fall within the range of -180 to 180 degrees.

        Args:
            a (float): The angle in degrees to be normalized.

        Returns:
            float: The input angle adjusted to fall within the range of -180 to 180 degrees.
        """
        return ((a + 180) % 360) - 180

    @staticmethod
    def normal_distribution(x: float, mean: float, sd: float, return_type: str) -> Union[np.array, list]:
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

        if return_type == "np_array":
            return n_dist

        elif return_type == "list":
            return n_dist.tolist()

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
        a = SignalStrengthModeling.normalize_angle(a)
        inflate = -db_back / (
            SignalStrengthModeling.normal_distribution(0, 0, sd, "np_array")
            - SignalStrengthModeling.normal_distribution(180, 0, sd, "np_array")
        )
        return (
            SignalStrengthModeling.normal_distribution(a, 0, sd, "np_array")
            - SignalStrengthModeling.normal_distribution(0, 0, sd, "np_array")
        ) * inflate

    @staticmethod
    @F.udf(FloatType())
    def norm_dBloss_udf(a, sd, db_back):
        a = SignalStrengthModeling.normalize_angle(a)  # You need to define this function
        inflate = -db_back / (
            SignalStrengthModeling.normal_distribution(0, 0, sd, "list")
            - SignalStrengthModeling.normal_distribution(180, 0, sd, "list")
        )
        return (
            SignalStrengthModeling.normal_distribution(a, 0, sd, "list")
            - SignalStrengthModeling.normal_distribution(0, 0, sd, "list")
        ) * inflate

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
        df["dbLoss"] = SignalStrengthModeling.norm_dBloss(df["a"], db_back=db_back, sd=sd)
        return df.loc[np.abs(-3 - df["dbLoss"]).idxmin(), "a"]

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
        idf["deg"] = idf["sd"].apply(SignalStrengthModeling.get_min3db, db_back=db_back)
        df = pd.DataFrame({"deg": np.arange(1, 181)})
        df["sd"] = df["deg"].apply(lambda dg: idf.loc[np.abs(idf["deg"] - dg).idxmin(), "sd"])
        df[signal_front_back_difference_col] = db_back
        return df

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
        sdf = sdf.withColumn(
            ColNames.signal_strength,
            F.col(ColNames.signal_strength)
            + SignalStrengthModeling.norm_dBloss_udf(
                F.col("azim2"),
                F.col("sd_azimuth"),
                F.col(ColNames.azimuth_signal_strength_back_loss),
            ),
        )

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

        # get power adjustments
        sdf = sdf.withColumn(
            ColNames.signal_strength,
            F.col(ColNames.signal_strength)
            + SignalStrengthModeling.norm_dBloss_udf(
                F.col("elev"),
                F.col("sd_elevation"),
                F.col(ColNames.elevation_signal_strength_back_loss),
            ),
        )

        # cleanup
        sdf = sdf.drop("gamma_elev", "elev", "sd_elevation")

        return sdf
