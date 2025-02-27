"""
This module provides functionality for generating a grid based on the INSPIRE grid system specification.
"""

from math import log10
from abc import ABCMeta, abstractmethod
from typing import List, Tuple, Union
from multimno.core.constants.columns import ColNames
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, LongType
from pyproj import Transformer
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP

from multimno.core.constants.columns import ColNames


class GridGenerator(metaclass=ABCMeta):
    """
    Abstract class that provides functionality for generating a grid.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark: SparkSession = spark

    @abstractmethod
    def cover_extent_with_grid_ids(self, extent: tuple) -> DataFrame:
        """Cover given extent with grid_ids on given resolution."""

    @abstractmethod
    def cover_polygon_with_grid_ids(self, polygon_sdf: DataFrame) -> DataFrame:
        """Cover given polygon with grid_ids on given resolution."""

    @abstractmethod
    def grid_ids_to_centroids(self, sdf: DataFrame, to_crs: int) -> DataFrame:
        """Get geometry centroids from grid_ids with given coordinate system."""

    @abstractmethod
    def grid_ids_to_tiles(self, sdf: DataFrame, to_crs: int) -> DataFrame:
        """Get grid polygons from grid_ids with given coordinate system."""

    @abstractmethod
    def get_parent_grid_ids(self, sdf: DataFrame, resolution: int) -> DataFrame:
        """Get parent grid_id on given resolution."""

    @abstractmethod
    def get_children_grid_ids(self, sdf: DataFrame, resolution: int) -> DataFrame:
        """Get children grid_ids on given resolution."""


class InspireGridGenerator(GridGenerator):
    """A class used to generate a grid based on the INSPIRE grid system specification.

    Attributes:
        GRID_CRS_EPSG_CODE (int): The EPSG code for the grid's CRS.
    """

    GRID_CRS_EPSG_CODE = 3035
    PROJ_COORD_INT_SIZE = 7

    def __init__(
        self,
        spark: SparkSession,
        resolution=100,
        geometry_col_name: str = "geometry",
        grid_id_col_name: str = "grid_id",
        grid_partition_size: int = 2000,
    ) -> None:
        """Initializes the InspireGridGenerator with the given parameters.

        Args:
            spark (SparkSession): The SparkSession to use.
            resolution (int, optional): The resolution of the grid. Defaults to 100. Has to be divisible by 100.
            geometry_col_name (str, optional): The name of the geometry column. Defaults to 'geometry'.
            grid_id_col_name (str, optional): The name of the grid ID column. Defaults to 'grid_id'.
            grid_partition_size (int, optional): The size of the grid partitions, defined as number of tiles
                in x and y dimensions of subdivisions of the intital grid. Defaults to 2000.

        Raises:
            ValueError: If the resolution is not divisible by 100.
        """
        if resolution % 100 != 0:
            raise ValueError("Resolution must be divisible by 100")

        super().__init__(spark)
        self.geometry_col_name = geometry_col_name
        self.grid_id_col_name = grid_id_col_name
        self.resolution = resolution
        self.grid_partition_size = grid_partition_size
        self.resolution_str = self._format_distance(resolution)

    @staticmethod
    def _format_distance(value: int) -> str:
        """Formats the given distance value to string.

        Args:
            value (int): The distance value to format.

        Returns:
            str: The formatted distance value.
        """
        if value < 1000:
            return f"{value}m"
        else:
            if value % 1000 != 0:
                raise ValueError(f"Distance to be formatted not multiple of 1000: {value}")
            return f"{value // 1000}km"

    def _project_latlon_extent(self, extent: List[float]) -> Union[List[float], List[float]]:
        """Projects the given extent from lat/lon to the grid's CRS.

        Args:
            extent (List[float]): The extent to project. Order: [lon_min, lat_min, lon_max, lat_max]

        Returns:
            List[float]: The projected extent.
        """
        transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{self.GRID_CRS_EPSG_CODE}")
        # EPSG4326: xx -> lat, yy -> lon
        # EPSG3035: xx -> northing, yy -> easting
        xx_bottomleft, yy_bottomleft = transformer.transform(extent[1], extent[0])  # bottom-left corner
        xx_topright, yy_topright = transformer.transform(extent[3], extent[2])  # top-right corner
        xx_bottomright, yy_bottomright = transformer.transform(extent[1], extent[2])  # bottom-right corner
        xx_topleft, yy_topleft = transformer.transform(extent[3], extent[0])

        return (
            [xx_bottomleft, yy_bottomleft, xx_topright, yy_topright],
            [xx_bottomright, yy_bottomright, xx_topleft, yy_topleft],
        )

    @staticmethod
    def _project_bounding_box(extent: List[float], auxiliar_coords: List[float]) -> Tuple[List[float], List[float]]:
        """Returns the bottom-left and top-right coordinates of the rectangular bounding box in the projected CRS
        that covers the bounding box defined from the bottom-left and top-right corners in lat/lon.

        Args:
            extent (List[float]): Coordinates in the projected CRS that are the transformation of the minimum and
                maximum latitude and longitude, in [x_bottomleft, y_bottomleft, x_topright, y_topright] order.
            auxiliar_coords (List[float]): Auxiliar coordinates in the prohected CRS that are the transformation
                of the other two cornes of the lat/lon rectangular bounding box, in
                [x_bottomright, y_bottomright, x_topleft, y_topleft] order

        Returns:
            List[float]: The projected extent, in [x_bottomleft, y_bottomleft, x_topright, y_topright] order.
            List[float]: Raster cover bounds, in [x_topleft, y_topleft, x_bottomright, y_bottomright] order.
        """
        cover_x_bottomleft = min(extent[0], auxiliar_coords[0])  # min lat
        cover_y_bottomleft = min(extent[1], auxiliar_coords[3])  # min lon

        cover_x_topright = max(extent[2], auxiliar_coords[2])  # max lat
        cover_y_topright = max(extent[3], auxiliar_coords[1])  # max lon

        cover_x_topleft = max(extent[2], auxiliar_coords[2])  # max lat
        cover_y_topleft = min(extent[1], auxiliar_coords[3])  # min lon

        cover_x_bottomright = min(extent[0], auxiliar_coords[0])  # min lat
        cover_y_bottomright = max(extent[3], auxiliar_coords[1])  # max lon

        return (
            [cover_y_bottomleft, cover_x_bottomleft, cover_y_topright, cover_x_topright],
            [cover_x_topleft, cover_y_topleft, cover_x_bottomright, cover_y_bottomright],
        )

    def _snap_extent_to_grid(self, extent: List[float]) -> List[float]:
        """Snaps the given extent to the grid.

        Args:
            extent (List[float]): The extent to snap.

        Returns:
            List[float]: The snapped extent.
        """
        return [round(coord / self.resolution) * self.resolution for coord in extent]

    def _extend_grid_extent(self, extent: List[float], extension_factor: int = 5) -> List[float]:
        """Extends the given extent by the specified factor in all directions.

        Args:
            extent (List[float]): The extent to extend.
            extension_factor (int, optional): The factor by which to extend the extent. Defaults to 5.

        Returns:
            List[float]: The extended extent.
        """
        extension_size = self.resolution * extension_factor
        return [
            extent[0] - extension_size,
            extent[1] - extension_size,
            extent[2] + extension_size,
            extent[3] + extension_size,
        ]

    def _extend_grid_raster_bounds(self, raster_bounds: List[float], extension_factor: int = 5) -> List[float]:
        """Extends the given extent by the specified factor in all directions.

        Args:.
            extent (List[float]): Raster cover bounds, in [x_topleft, y_topleft, x_bottomright, y_bottomright] order.

            extension_factor (int, optional): The factor by which to extend the extent. Defaults to 5.

        Returns:
            List[float]: The extended extent.
        """
        extension_size = self.resolution * extension_factor
        return [
            raster_bounds[0] + extension_size,  # x topleft
            raster_bounds[1] - extension_size,  # y topleft
            raster_bounds[2] - extension_size,  # x bottomright
            raster_bounds[3] + extension_size,  # y bottomright
        ]

    def _get_grid_height(self, raster_bounds: List[float]) -> int:
        """Calculates the height of the grid for the given extent.

        Args:
            raster_bounds (List[float]): The raster_bounds for which to calculate the grid height.

        Returns:
            int: The grid height.
        """
        return int((raster_bounds[0] - raster_bounds[2]) / self.resolution)

    def _get_grid_width(self, raster_bounds: List[float]) -> int:
        """Calculates the width of the grid for the given extent.

        Args:
            raster_bounds (List[float]): The raster_bounds for which to calculate the grid width.

        Returns:
            int: The grid width.
        """
        return int((raster_bounds[3] - raster_bounds[1]) / self.resolution)

    def _get_grid_blueprint(self, extent: List[float]) -> DataFrame:
        """Generates a blueprint for the grid for the given extent as a raster of grid resolution.
        Splits initial raster into smaller rasters of size grid_partition_size x grid_partition_size.

        Args:
            extent (List[float]): The extent for which to generate the grid blueprint.

        Returns:
            DataFrame: The grid blueprint.
        """
        extent, auxiliar_coords = self._project_latlon_extent(extent)
        extent, raster_bounds = self._project_bounding_box(extent, auxiliar_coords)

        extent = self._snap_extent_to_grid(extent)
        raster_bounds = self._snap_extent_to_grid(raster_bounds)

        extent = self._extend_grid_extent(extent)
        raster_bounds = self._extend_grid_raster_bounds(raster_bounds)

        grid_height = self._get_grid_height(raster_bounds)
        grid_width = self._get_grid_width(raster_bounds)

        # ONLY FOR EPSG:3035!!!! which has (northing, easting) order, BUT (Y, X) axis names in its EPSG (not in code)
        # raster_bounds[1]: easting of the top left corner. "X" axis
        # raster_bounds[0]: northing of the top left corner. "Y" axis

        sdf = self.spark.sql(
            f"""SELECT RS_MakeEmptyRaster(1, "B", {grid_width}, 
                                {grid_height}, 
                                {raster_bounds[1]},
                                {raster_bounds[0]}, 
                                {self.resolution}, 
                               -{self.resolution}, 0.0, 0.0, {self.GRID_CRS_EPSG_CODE}) as raster"""
        )

        sdf = sdf.selectExpr(f"RS_TileExplode(raster,{self.grid_partition_size}, {self.grid_partition_size})")
        return sdf.repartition(sdf.count())

    @staticmethod
    def _get_polygon_sdf_extent(polygon_sdf: DataFrame) -> List[float]:
        """Gets the extent of the given polygon DataFrame.

        Args:
            polygon_sdf (DataFrame): The polygon DataFrame.

        Returns:
            List[float]: The extent of the polygon DataFrame.
        """
        polygon_sdf = polygon_sdf.withColumn("bbox", STF.ST_Envelope(polygon_sdf["geometry"]))
        polygon_sdf = (
            polygon_sdf.withColumn("x_min", STF.ST_XMin(polygon_sdf["bbox"]))
            .withColumn("y_min", STF.ST_YMin(polygon_sdf["bbox"]))
            .withColumn("x_max", STF.ST_XMax(polygon_sdf["bbox"]))
            .withColumn("y_max", STF.ST_YMax(polygon_sdf["bbox"]))
        )

        return polygon_sdf.select("x_min", "y_min", "x_max", "y_max").collect()[0][0:]

    def _get_grid_intersection_with_mask(self, sdf: DataFrame, polygon_sdf: DataFrame) -> DataFrame:
        """Gets the intersection of the grid with the given mask.

        Args:
            sdf (DataFrame): The DataFrame representing the grid.
            polygon_sdf (DataFrame): The DataFrame representing the mask.

        Returns:
            DataFrame: The DataFrame representing the intersection of the grid with the mask.
        """
        polygon_sdf = polygon_sdf.withColumn(
            "geometry",
            STF.ST_Transform(polygon_sdf["geometry"], F.lit("EPSG:4326"), F.lit(f"EPSG:{self.GRID_CRS_EPSG_CODE}")),
        )

        sdf = sdf.join(
            polygon_sdf, STP.ST_Intersects(sdf[self.geometry_col_name], polygon_sdf["geometry"]), "inner"
        ).drop(polygon_sdf["geometry"])

        return sdf

    def _get_grid_intersection_with_mask(self, sdf: DataFrame, polygon_sdf: DataFrame) -> DataFrame:
        """Covers the given extent with grid centroids.

        Args:
            extent (List[float]): The extent to cover.

        Returns:
            DataFrame: The DataFrame representing the grid centroids covering the extent.
        """
        polygon_sdf = polygon_sdf.withColumn(
            "geometry",
            STF.ST_Transform(polygon_sdf["geometry"], F.lit("EPSG:4326"), F.lit(f"EPSG:{self.GRID_CRS_EPSG_CODE}")),
        )

        sdf = sdf.join(
            polygon_sdf, STP.ST_Intersects(sdf[self.geometry_col_name], polygon_sdf["geometry"]), "inner"
        ).drop(polygon_sdf["geometry"])

        return sdf

    def cover_extent_with_grid_centroids(self, extent: List[float]) -> DataFrame:
        """Covers the given polygon with grid centroids.

        Args:
            extent (List[float]): The extent to cover.

        Returns:
            DataFrame: The DataFrame representing the grid centroids covering the polygon.
        """
        sdf = self._get_grid_blueprint(extent)

        sdf = sdf.selectExpr("explode(RS_PixelAsCentroids(tile, 1)) as exploded").selectExpr(
            f"exploded.geom as {self.geometry_col_name}"
        )

        sdf = sdf.withColumn(
            self.grid_id_col_name,
            (
                (STF.ST_Y(sdf["geometry"]) - (self.resolution / 2)).cast(LongType()) * 10**self.PROJ_COORD_INT_SIZE
                + (STF.ST_X(sdf["geometry"]) - (self.resolution / 2)).cast(LongType())
            ).cast(LongType()),
        )

        return sdf

    def cover_polygon_with_grid_centroids(self, polygon_sdf: DataFrame) -> DataFrame:
        """Covers the given extent with grid IDs.

        Args:
            polygon_sdf (DataFrame): The DataFrame representing the polygon.

        Returns:
            DataFrame: The DataFrame representing the grid IDs covering the extent.
        """
        extent = self._get_polygon_sdf_extent(polygon_sdf)

        sdf = self.cover_extent_with_grid_centroids(extent)

        sdf = self._get_grid_intersection_with_mask(sdf, polygon_sdf)

        return sdf

    def cover_extent_with_grid_ids(self, extent: List[float]) -> DataFrame:
        """Covers the given polygon with grid IDs.

        Args:
            extent (List[float]): The extent to cover.

        Returns:
            DataFrame: The DataFrame representing the grid IDs covering the polygon.
        """
        sdf = self.cover_extent_with_grid_centroids(extent)

        sdf = sdf.drop(self.geometry_col_name)

        return sdf

    def cover_polygon_with_grid_ids(self, polygon_sdf: DataFrame) -> DataFrame:
        """Covers the given polygon with grid IDs.

        Args:
            polygon_sdf (DataFrame): The DataFrame representing the polygon.

        Returns:
            DataFrame: The DataFrame representing the grid IDs covering the polygon.
        """
        extent = self._get_polygon_sdf_extent(polygon_sdf)

        sdf = self.cover_extent_with_grid_centroids(extent)

        sdf = self._get_grid_intersection_with_mask(sdf, polygon_sdf)

        sdf = sdf.drop("geometry")

        return sdf

    def cover_extent_with_grid_tiles(self, extent: List[float]) -> DataFrame:
        """Covers the given extent with grid tiles.

        Args:
            extent (List[float]): The extent to cover.

        Returns:
            DataFrame: The DataFrame representing the grid tiles covering the extent.
        """
        sdf = self._get_grid_blueprint(extent)

        sdf = sdf.selectExpr("explode(RS_PixelAsPolygons(tile, 1)) as exploded").selectExpr(
            f"exploded.geom as {self.geometry_col_name}"
        )

        sdf = sdf.withColumn(
            self.grid_id_col_name,
            (
                STF.ST_YMin(sdf["geometry"]).cast(LongType()) * 10**self.PROJ_COORD_INT_SIZE
                + STF.ST_XMin(sdf["geometry"])
            ).cast(LongType()),
        )

        return sdf

    def cover_polygon_with_grid_tiles(self, polygon_sdf: DataFrame) -> DataFrame:
        """Covers the given polygon with grid tiles.

        Args:
            polygon_sdf (DataFrame): The DataFrame representing the polygon.

        Returns:
            DataFrame: The DataFrame representing the grid tiles covering the polygon.
        """
        extent = self._get_polygon_sdf_extent(polygon_sdf)

        sdf = self.cover_extent_with_grid_tiles(extent)

        sdf = self._get_grid_intersection_with_mask(sdf, polygon_sdf)

        return sdf

    def convert_internal_id_to_inspire_specs(
        self, sdf: DataFrame, resolution: int = None, grid_id_col: str = None
    ) -> DataFrame:
        """
        Converts the integer grid id column in a DataFrame to the INSPIRE grid string format.

        Args:
            sdf (DataFrame): Input DataFrame with a column grid_id_col in integer format.
            resolution (int, optional): The resolution of the grid. If None, it is equal to self.resolution. Defaults
                to None
            grid_id_col (str, optional): name of the column containing the integer grid id column. If None, it is
                equal to self.grid_id_col_name. Defaults to None.

        Returns:
            DataFrame: A new DataFrame with the grid_id converted to string format.
        """
        if resolution is None:
            resolution = self.resolution
            resolution_str = self.resolution_str
        else:
            if resolution % 100 != 0:
                raise ValueError(f"Invalid resolution value not divisible by 100: {resolution}")
            resolution_str = self._format_distance(resolution)

        if grid_id_col is None:
            grid_id_col = self.grid_id_col_name

        trailing_zeros = int(log10(resolution))

        sdf = sdf.withColumn(
            grid_id_col,
            F.concat(
                F.lit(resolution_str),
                F.lit("N"),
                (F.expr(f"{grid_id_col} DIV {10**(self.PROJ_COORD_INT_SIZE + trailing_zeros)}")),
                F.lit("E"),
                (F.expr(f"({grid_id_col} % {10**self.PROJ_COORD_INT_SIZE}) DIV {10 ** trailing_zeros}")),
            ),
        )

        return sdf

    def grid_ids_to_centroids(self, sdf: DataFrame, resolution: int = None, to_crs: int = None) -> DataFrame:
        """Converts grid IDs to centroids.

        Args:
            sdf (DataFrame): The DataFrame containing the grid IDs.
            to_crs (int, optional): The CRS to which to convert the centroids. If not provided,
                the centroids will be in default grid crs.

        Returns:
            DataFrame: The DataFrame containing the centroids.
        """
        if resolution is None:
            resolution = self.resolution

        sdf = sdf.withColumn(
            self.geometry_col_name,
            STC.ST_Point(
                F.expr(f"INT({self.grid_id_col_name} % {10**self.PROJ_COORD_INT_SIZE})") + resolution / 2,
                F.expr(f"INT({self.grid_id_col_name} DIV {10**self.PROJ_COORD_INT_SIZE})") + resolution / 2,
            ),
        )

        sdf = sdf.withColumn(
            self.geometry_col_name, STF.ST_SetSRID(sdf[self.geometry_col_name], self.GRID_CRS_EPSG_CODE)
        )

        if to_crs:
            sdf = sdf.withColumn(
                self.geometry_col_name, STF.ST_Transform(sdf[self.geometry_col_name], F.lit(f"EPSG:{to_crs}"))
            )

        return sdf

    def grid_ids_to_tiles(self, sdf: DataFrame, resolution: int = None, to_crs: int = None) -> DataFrame:
        """Converts grid IDs in INSPIRE format to tiles.

        Args:
            sdf (DataFrame): The DataFrame containing the grid IDs.
            to_crs (int, optional): The CRS to which to convert the tiles. If not provided, the centroids
                will be in default grid crs.
        Returns:
            DataFrame: The DataFrame containing the tiles.
        """

        if resolution is None:
            resolution = self.resolution

        sdf = sdf.withColumn(
            self.geometry_col_name,
            STC.ST_PolygonFromEnvelope(
                F.expr(f"{self.grid_id_col_name} % {10**self.PROJ_COORD_INT_SIZE}"),  # x/easting min
                F.expr(f"{self.grid_id_col_name} DIV {10**self.PROJ_COORD_INT_SIZE}"),  # y/northing min
                F.expr(f"{self.grid_id_col_name} % {10**self.PROJ_COORD_INT_SIZE}") + resolution,  # x/easting max
                F.expr(f"{self.grid_id_col_name} DIV {10**self.PROJ_COORD_INT_SIZE}") + resolution,  # y/northing max
            ),
        )

        sdf = sdf.withColumn(
            self.geometry_col_name, STF.ST_SetSRID(sdf[self.geometry_col_name], self.GRID_CRS_EPSG_CODE)
        )

        if to_crs:
            sdf = sdf.withColumn(
                self.geometry_col_name, STF.ST_Transform(sdf[self.geometry_col_name], F.lit(f"EPSG:{to_crs}"))
            )

        return sdf

    def get_parent_grid_ids(self, sdf: DataFrame, resolution: int, parent_col_name: str = None) -> DataFrame:
        """
        Coarsens grid IDs to the specified resolution, snapping down to the nearest coarser grid cell.

        Args:
            sdf (DataFrame): Input Spark DataFrame with a 14-digit grid ID column named 'grid_id'.
            resolution (int): The desired coarser resolution (e.g., 200).
            parent_col_name (str, optional): name of the column that will hold the parent grid IDs. If None, it will be
                named f"parent_{ColNames.grid_id}". Defaults to None.

        Returns:
            DataFrame: DataFrame with coarsened grid IDs in new column.
        """
        if resolution % 100 != 0:
            raise ValueError("Resolution must be a multiple of 100.")
        if parent_col_name is None:
            parent_col_name = f"parent_{ColNames.grid_id}"

        # Extract `northing` and `easting` correctly
        sdf = sdf.withColumn("northing", F.expr(f"{ColNames.grid_id} DIV {10**self.PROJ_COORD_INT_SIZE}")).withColumn(
            "easting", F.col(ColNames.grid_id) % 10**self.PROJ_COORD_INT_SIZE
        )

        # Snap `northing` and `easting` down to the nearest coarser grid boundary
        sdf = sdf.withColumn("northing_parent", F.col("northing") - (F.col("northing") % resolution)).withColumn(
            "easting_parent", F.col("easting") - (F.col("easting") % resolution)
        )

        # Combine the coarsened `northing` and `easting` into `parent_grid_id`
        sdf = sdf.withColumn(
            parent_col_name,
            (F.col("northing_parent").cast(LongType()) * 10**self.PROJ_COORD_INT_SIZE + F.col("easting_parent")),
        ).drop("northing", "easting", "northing_parent", "easting_parent")

        return sdf

    def get_children_grid_ids(
        self, sdf: DataFrame, resolution: int, child_resolution: int, child_col_name: str = None
    ) -> DataFrame:
        """
        Expands coarsened grid IDs to generate all possible child grid IDs within the specified finer resolution.

        Args:
            sdf (DataFrame): Input Spark DataFrame with a 14-digit grid ID column named 'grid_id'.
            resolution (int): The coarser resolution (e.g., 200).
            child_resolution (int): The finer resolution for child grids (e.g., 100).
            child_col_name (str, optional): name of the column that will hold the children grid IDs. If None, it will be
                named f"child_{ColNames.grid_id}". Defaults to None.

        Returns:
            DataFrame: DataFrame with generated child grid IDs in new column.
        """
        if resolution % 100 != 0 or child_resolution % 100 != 0:
            raise ValueError("Both resolutions must be multiples of 100.")
        if child_resolution >= resolution:
            raise ValueError("Child resolution must be finer than the parent resolution.")
        if resolution % child_resolution != 0:
            raise ValueError("Parent resolution must be a multiple of the child resolution.")
        if child_col_name is None:
            child_col_name = f"child_{ColNames.grid_id}"

        factor = resolution // child_resolution

        # Extract `northing` and `easting` from the parent grid ID
        sdf = sdf.withColumn("northing", F.expr(f"{ColNames.grid_id} DIV {10**self.PROJ_COORD_INT_SIZE}")).withColumn(
            "easting", F.col(ColNames.grid_id) % 10**self.PROJ_COORD_INT_SIZE
        )

        # Generate all possible offsets for child grids
        offsets = [(i, j) for i in range(factor) for j in range(factor)]
        spark = sdf.sparkSession  # Get the current Spark session
        offsets_df = spark.createDataFrame(offsets, ["northing_offset", "easting_offset"])

        # Cross join with the offsets to generate all child grid IDs
        result = (
            sdf.crossJoin(offsets_df)
            .withColumn("child_northing", F.expr(f"northing + northing_offset * {child_resolution}"))
            .withColumn("child_easting", F.expr(f"easting + easting_offset * {child_resolution}"))
            .withColumn(
                child_col_name,
                (F.col("child_northing").cast(LongType()) * 10**self.PROJ_COORD_INT_SIZE + F.col("child_easting")),
            )
            .drop("child_northing", "child_easting", "northing", "easting", "northing_offset", "easting_offset")
        )

        return result
