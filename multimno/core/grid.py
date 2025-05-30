"""
This module provides functionality for generating a grid based on the INSPIRE grid system specification.
"""

from typing import List, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StructField, StructType
from pyproj import Transformer
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP

from multimno.core.constants.columns import ColNames


class InspireGridGenerator:
    """A class used to generate a grid based on the INSPIRE 100m grid system specification."""

    GRID_CRS_EPSG_CODE = 3035
    ACCEPTED_RESOLUTIONS = [100, 1000]

    def __init__(
        self,
        spark: SparkSession,
        geometry_col: str = ColNames.geometry,
        grid_id_col: str = ColNames.grid_id,
        grid_partition_size: int = 2000,
    ):
        self.spark = spark
        self.resolution = 100
        self.resolution_str = self._format_distance(self.resolution)
        self.geometry_col = geometry_col
        self.grid_id_col = grid_id_col
        self.grid_partition_size = grid_partition_size

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

    def _project_latlon_extent(self, extent: List[float]) -> Tuple[List[float], List[float]]:
        """Projects the given extent from lat/lon to the grid's CRS.

        Args:
            extent (List[float]): The extent to project. Order: [lon_min, lat_min, lon_max, lat_max]

        Returns:
            List[float]: The projected extent. Order: [northing_bottomleft, easting_bottomleft, northing_topright,
                easting_topright]
            list[float]: Auxiliar coordinates. Order: [northing_bottomright, easting_bottomright, northing_topleft,
                easting_topleft]
        """
        transformer = Transformer.from_crs("EPSG:4326", f"EPSG:{self.GRID_CRS_EPSG_CODE}")
        # This transformer follows the following convention (xx, yy are the first and second coords, respetively)
        # EPSG4326: xx -> lat, yy -> lon
        # EPSG3035: xx -> northing, yy -> easting
        nn_bottomleft, ee_bottomleft = transformer.transform(extent[1], extent[0])  # bottom-left corner
        nn_topright, ee_topright = transformer.transform(extent[3], extent[2])  # top-right corner
        nn_bottomright, ee_bottomright = transformer.transform(extent[1], extent[2])  # bottom-right corner
        nn_topleft, ee_topleft = transformer.transform(extent[3], extent[0])

        return (
            [nn_bottomleft, ee_bottomleft, nn_topright, ee_topright],
            [nn_bottomright, ee_bottomright, nn_topleft, ee_topleft],
        )

    @staticmethod
    def _project_bounding_box(extent: List[float], auxiliar_coords: List[float]) -> Tuple[List[float], List[float]]:
        """Returns the bottom-left and top-right coordinates of the rectangular bounding box in the projected CRS
        that covers the bounding box defined from the bottom-left and top-right corners in lat/lon.

        Args:
            extent (list[float]): Coordinates in the projected CRS that are the transformation of the minimum and
                maximum latitude and longitude, in [n_bottomleft, e_bottomleft, n_topright, e_topright] order.
            auxiliar_coords (list[float]): Auxiliar coordinates in the projected CRS that are the transformation
                of the other two corners of the rectangular bounding box, in
                [n_bottomright, e_bottomright, n_topleft, e_topleft] order

        Returns:
            list[float]: The projected extent, in [n_bottomleft, e_bottomleft, n_topright, e_topright] order.
            list[float]: Raster cover bounds, in [n_topleft, e_topleft, n_bottomright, e_bottomright] order.
        """
        cover_n_bottomleft = min(extent[0], auxiliar_coords[0])  # min lat
        cover_e_bottomleft = min(extent[1], auxiliar_coords[3])  # min lon

        cover_n_topright = max(extent[2], auxiliar_coords[2])  # max lat
        cover_e_topright = max(extent[3], auxiliar_coords[1])  # max lon

        cover_n_topleft = max(extent[2], auxiliar_coords[2])  # max lat
        cover_e_topleft = min(extent[1], auxiliar_coords[3])  # min lon

        cover_n_bottomright = min(extent[0], auxiliar_coords[0])  # min lat
        cover_e_bottomright = max(extent[3], auxiliar_coords[1])  # max lon

        return (
            [cover_n_bottomleft, cover_e_bottomleft, cover_e_topright, cover_n_topright],
            [cover_n_topleft, cover_e_topleft, cover_n_bottomright, cover_e_bottomright],
        )

    def _snap_extent_to_grid(self, extent: List[float]) -> List[float]:
        """Snaps the given extent to the grid.

        Args:
            extent (list[float]): The extent to snap.

        Returns:
            list[float]: The snapped extent.
        """
        return [round(coord / self.resolution) * self.resolution for coord in extent]

    def _extend_grid_extent(self, extent: List[float], extension_factor: int = 5) -> List[float]:
        """Extends the given extent by the specified factor in all directions.

        Args:
            extent (list[float]): The extent to extend.
            extension_factor (int, optional): The factor by which to extend the extent. Defaults to 5.

        Returns:
            list[float]: The extended extent.
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
            extent (list[float]): Raster cover bounds, in [x_topleft, y_topleft, x_bottomright, y_bottomright] order.
            extension_factor (int, optional): The factor by which to extend the extent. Defaults to 5.

        Returns:
            list[float]: The extended extent.
        """
        extension_size = self.resolution * extension_factor
        return [
            raster_bounds[0] + extension_size,  # n topleft
            raster_bounds[1] - extension_size,  # e topleft
            raster_bounds[2] - extension_size,  # n bottomright
            raster_bounds[3] + extension_size,  # e bottomright
        ]

    def _get_grid_height(self, raster_bounds: List[float]) -> int:
        """Calculates the height of the grid for the given extent.

        Args:
            raster_bounds (list[float]): The raster_bounds for which to calculate the grid height.

        Returns:
            int: The grid height.
        """
        return int((raster_bounds[0] - raster_bounds[2]) / self.resolution)

    def _get_grid_width(self, raster_bounds: List[float]) -> int:
        """Calculates the width of the grid for the given extent.

        Args:
            raster_bounds (list[float]): The raster_bounds for which to calculate the grid width.

        Returns:
            int: The grid width.
        """
        return int((raster_bounds[3] - raster_bounds[1]) / self.resolution)

    def process_latlon_extent(self, extent: List[float]) -> Tuple[List[float], List[float]]:
        """Takes an extent expressed in latitude and longitude (EPSG 4326), projects it into EPSG 3035, creates
        bounding box, snaps to grid, and extends it some extra tiles in each direction.

        Args:
            extent (list[float]): The extent in lat/lon to process. Ordering is [lon_min, lat_min, lon_max, lat_max].

        Returns:
            extent (list[float]): Coordinates of the rectangle/bounding box that covers the projected and extended
                extent. Order is [n_min, e_min, n_max, e_max] (bottom-left and top-right corners)
            raster_bounds (list[float]): Appropriate raster bounds
        """
        extent, auxiliar_coords = self._project_latlon_extent(extent)
        extent, raster_bounds = self._project_bounding_box(extent, auxiliar_coords)

        extent = self._snap_extent_to_grid(extent)
        raster_bounds = self._snap_extent_to_grid(raster_bounds)

        extent = self._extend_grid_extent(extent)
        raster_bounds = self._extend_grid_raster_bounds(raster_bounds)
        return extent, raster_bounds

    def _get_grid_blueprint(self, extent: List[float]) -> Tuple[DataFrame, List[float]]:
        """Generates a blueprint for the grid for the given extent as a raster of grid resolution.
        Splits initial raster into smaller rasters of size grid_partition_size x grid_partition_size.

        Args:
            extent (List[float]): The extent in lat/lon for which to generate the grid blueprint. Ordering must be
                [lon_min, lat_min, lon_max, lat_max].

        Returns:
            DataFrame: The grid blueprint.
            proj_extent (List[float]): Coordinates of the rectangle/bounding box that covers the projected and extended
                extent. Order is [n_min, e_min, n_max, e_max] (bottom-left and top-right corners)
        """
        proj_extent, raster_bounds = self.process_latlon_extent(extent)

        grid_height = self._get_grid_height(raster_bounds)
        grid_width = self._get_grid_width(raster_bounds)

        sdf = self.spark.sql(
            f"""SELECT RS_MakeEmptyRaster(1, "B", {grid_width}, 
                                {grid_height}, 
                                {raster_bounds[1]},
                                {raster_bounds[0]}, 
                                {self.resolution}, 
                               -{self.resolution}, 0.0, 0.0, {self.GRID_CRS_EPSG_CODE}) as raster"""
        )

        sdf = sdf.selectExpr(f"RS_TileExplode(raster,{self.grid_partition_size}, {self.grid_partition_size})")
        return sdf.repartition(sdf.count()), proj_extent

    @staticmethod
    def _get_polygon_sdf_extent(polygon_sdf: DataFrame) -> List[float]:
        """Gets the extent of the given polygon DataFrame. This method is currently used with geometry in
        EPSG 4326, following order used by Sedona where the first coordinate X contains longitude and the second
            coordinate Y contains latitude.

        Args:
            polygon_sdf (DataFrame): The polygon DataFrame.

        Returns:
            list[float]: The extent of the polygon DataFrame. Order: [x_min, y_min, x_max, y_max] where x and y
                refer to the horizontal (longitude) and vertical (latitude) coordinates of the geometry, respectively.
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
        """Gets the intersection of the grid with the given polygon mask.

        Args:
            sdf (DataFrame): The DataFrame representing the grid.
            polygon_sdf (DataFrame): The DataFrame representing the mask in EPSG:4326. Sedona function expects
                first coordinate (X) to be longitude and second coordinate (Y) to be latitude.

        Returns:
            DataFrame: The DataFrame representing the intersection of the grid with the mask.
        """
        polygon_sdf = polygon_sdf.withColumn(
            "geometry",
            STF.ST_Transform(polygon_sdf["geometry"], F.lit("EPSG:4326"), F.lit(f"EPSG:{self.GRID_CRS_EPSG_CODE}")),
        )

        sdf = sdf.join(polygon_sdf, STP.ST_Intersects(sdf[self.geometry_col], polygon_sdf["geometry"]), "inner").drop(
            polygon_sdf["geometry"]
        )

        return sdf

    def _get_grid_id_from_centroids(self, sdf: DataFrame, n_origin: int, e_origin: int) -> DataFrame:
        """Takes a DataFrame that has point geometries in [self.geometry_col] column in EPSG:3035, representing the
        centroids of grid tiles, and creates the internal unsigned 4-byte identifier used internally by the pipeline.

        The 4-byte identifier consists of two parts:
            The two most significant bytes represent the easting coordinate (horizontal coordinate of EPSG:3035)
            The two least significant bytes represent the northing coordinate (vertical coordinate of EPSG:3035)

        Sedona represents the X and Y coordinates as the horizontal and vertical coordinates, respectively. It does
        not follow the coordinate order of the CRS.

        In order to fit all the necessary tiles into 4 bytes, we do a traslation of the coordinate system to have
        a different origin, defined by (e_origin, n_origin)

        Args:
            sdf (DataFrame): DataFrame containing grid tile centroids to which we want to add the grid ID.
            n_origin (int): Northing origin to be used in the internal 4-byte ID. In metres.
            e_origin (int): Easting origin to be used in the internal 4-byte ID. In metres.

        Returns:
            DataFrame: DataFrame with a grid ID column added
        """
        sdf = sdf.withColumn(
            self.grid_id_col,
            (
                F.shiftleft(
                    ((STF.ST_X(F.col(self.geometry_col)) - e_origin - self.resolution / 2) / self.resolution).cast(
                        IntegerType()
                    ),
                    16,
                )
                + (
                    ((STF.ST_Y(F.col(self.geometry_col)) - n_origin - self.resolution / 2) / self.resolution).cast(
                        IntegerType()
                    )
                )
            ),
        )

        # Add origin column
        sdf = sdf.withColumn(
            ColNames.origin,
            (
                F.shiftleft(F.lit(e_origin / self.resolution).cast(LongType()), 32)
                + F.lit(n_origin / self.resolution).cast(LongType())
            ).cast(LongType()),
        )
        return sdf

    def _get_grid_id_from_grid_tiles(self, sdf: DataFrame, n_origin: int, e_origin: int) -> DataFrame:
        """Takes a DataFrame that has tile geometries in [self.geometry_col] column in EPSG:3035, representing the
        grid tiles, and creates the internal unsigned 4-byte identifier used internally by the pipeline.

        The 4-byte identifier consists of two parts:
            The two most significant bytes represent the easting coordinate (horizontal coordinate of EPSG:3035)
            The two least significant bytes represent the northing coordinate (vertical coordinate of EPSG:3035)

        Sedona represents the X and Y coordinates as the horizontal and vertical coordinates, respectively. It does
        not follow the coordinate order of the CRS.

        In order to fit all the necessary tiles into 4 bytes, we do a traslation of the coordinate system to have
        a different origin, defined by (e_origin, n_origin)

        Args:
            sdf (DataFrame): DataFrame containing grid tile centroids to which we want to add the grid ID.
            n_origin (int): Northing origin to be used in the internal 4-byte ID. In metres.
            e_origin (int): Easting origin to be used in the internal 4-byte ID. In metres.

        Returns:
            DataFrame: DataFrame with a grid ID column added
        """
        sdf = sdf.withColumn(
            self.grid_id_col,
            (
                F.shiftleft(
                    ((STF.ST_XMin(F.col(self.geometry_col)) - e_origin) / self.resolution).cast(IntegerType()), 16
                )
                + (((STF.ST_YMin(F.col(self.geometry_col)) - n_origin) / self.resolution).cast(IntegerType()))
            ),
        )

        # Add origin column
        sdf = sdf.withColumn(
            ColNames.origin,
            (
                F.shiftleft(F.lit(e_origin / self.resolution).cast(LongType()), 32)
                + F.lit(n_origin / self.resolution).cast(LongType())
            ).cast(LongType()),
        )
        return sdf

    def cover_extent_with_grid_centroids(
        self, extent: List[float], n_origin: int = None, e_origin: int = None
    ) -> DataFrame:
        """Covers the given extent with grid centroids. It takes an extent expressed in EPSG:4326 and covers it
        with grid centroid point geometries in EPSG:3035, returning a DataFrame with these geometries, the internal
        4-byte grid ID and the origin used to define the 4-byte ID. If both `n_origin` and `e_origin` are provided,
        they are used as the origin of the ID; if not, the origin is taken from the provided extent.

        It is desirable to define the origin using `n_origin` and `e_origin` when one wants to cover several extents
        sharing the same origin, i.e. using the 4-byte grid ID defined in the same way for all of them.

        Args:
            extent (list[float]): The extent in lat/lon (EPSG:4326) to cover with grid centroids. Ordering must be
                [lon_min, lat_min, lon_max, lat_max].
            n_origin (int, optional): northing origin to be used for the 4-byte grid ID, in EPSG:3035 (metres).
                Defaults to None.
            e_origin (int, optional): easting origin to be used for the 4-byte grid ID, in EPSG:3035 (metres).
                Defaults to None.

        Returns:
            DataFrame: The DataFrame representing the grid centroids covering the extent, with their grid ID and origin
                columns.
        """
        if (n_origin is None and e_origin is not None) or (n_origin is not None and e_origin is None):
            raise ValueError("Either both or none of the arguments `n_origin` and `e_origin` must be passed")

        sdf, proj_extent = self._get_grid_blueprint(extent)

        sdf = sdf.selectExpr("explode(RS_PixelAsCentroids(tile, 1)) as exploded").selectExpr(
            f"exploded.geom as {self.geometry_col}"
        )

        if n_origin is not None:
            sdf = self._get_grid_id_from_centroids(sdf, n_origin=n_origin, e_origin=e_origin)
        else:
            sdf = self._get_grid_id_from_centroids(sdf, n_origin=proj_extent[0], e_origin=proj_extent[1])

        return sdf

    def cover_polygon_with_grid_centroids(
        self, polygon_sdf: DataFrame, n_origin: int = None, e_origin: int = None
    ) -> DataFrame:
        """Covers the given polygon with grid centroids. It takes an polygon expressed in EPSG:4326 and covers it
        with grid centroid point geometries in EPSG:3035, returning a DataFrame with these geometries, the internal
        4-byte grid ID and the origin used to define the 4-byte ID. If both `n_origin` and `e_origin` are provided,
        they are used as the origin of the ID; if not, the origin is taken from the extent covering the provided
        polygon.

        It is desirable to define the origin using `n_origin` and `e_origin` when one wants to cover several polygons
        sharing the same origin, i.e. using the 4-byte grid ID defined in the same way for all of them.

        Args:
            polygon_sdf (DataFrame): DataFrame containing a single row with a polygon in EPSG:4326 in a column named
                `geometry`.
            n_origin (int, optional): northing origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.
            e_origin (int, optional): easting origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.

        Returns:
            DataFrame: The DataFrame representing the grid centroids covering the polygon, with their grid ID and origin
                columns.
        """
        extent = self._get_polygon_sdf_extent(polygon_sdf)

        sdf = self.cover_extent_with_grid_centroids(extent, n_origin, e_origin)

        sdf = self._get_grid_intersection_with_mask(sdf, polygon_sdf)

        return sdf

    def cover_extent_with_grid_tiles(
        self, extent: List[float], n_origin: int = None, e_origin: int = None
    ) -> Tuple[DataFrame, List[float]]:
        """Covers the given extent with grid tiles. It takes an extent expressed in EPSG:4326 and covers it
        with grid tile polygon geometries in EPSG:3035, returning a DataFrame with these geometries, the internal
        4-byte grid ID and the origin used to define the 4-byte ID. If both `n_origin` and `e_origin` are provided,
        they are used as the origin of the ID; if not, the origin is taken from the provided extent.

        It is desirable to define the origin using `n_origin` and `e_origin` when one wants to cover several extents
        sharing the same origin, i.e. using the 4-byte grid ID defined in the same way for all of them.

        Args:
            extent (list[float]): The extent in lat/lon (EPSG:4326) to cover with grid tiles. Ordering must be
                [lon_min, lat_min, lon_max, lat_max].
            n_origin (int, optional): northing origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.
            e_origin (int, optional): easting origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.

        Returns:
            DataFrame: The DataFrame representing the grid tiles covering the extent, with their grid ID and origin
                columns.
        """
        if (n_origin is None and e_origin is not None) or (n_origin is not None and e_origin is None):
            raise ValueError("Either both or none of the arguments `n_origin` and `e_origin` must be passed")

        sdf, proj_extent = self._get_grid_blueprint(extent)

        sdf = sdf.selectExpr("explode(RS_PixelAsPolygons(tile, 1)) as exploded").selectExpr(
            f"exploded.geom as {self.geometry_col}"
        )

        if n_origin is not None:
            sdf = self._get_grid_id_from_grid_tiles(sdf, n_origin=n_origin, e_origin=e_origin)
        else:
            sdf = self._get_grid_id_from_grid_tiles(sdf, n_origin=proj_extent[0], e_origin=proj_extent[1])

        return sdf

    def cover_polygon_with_grid_tiles(self, polygon_sdf: DataFrame, n_origin: int, e_origin: int) -> DataFrame:
        """Covers the given polygon with grid tiles. It takes an polygon expressed in EPSG:4326 and covers it
        with grid tile polygon geometries in EPSG:3035, returning a DataFrame with these geometries, the internal
        4-byte grid ID and the origin used to define the 4-byte ID. If both `n_origin` and `e_origin` are provided,
        they are used as the origin of the ID; if not, the origin is taken from the polygon covering the provided
        polygon.

        It is desirable to define the origin using `n_origin` and `e_origin` when one wants to cover several polygons
        sharing the same origin, i.e. using the 4-byte grid ID defined in the same way for all of them.

        Args:
            polygon_sdf (DataFrame): DataFrame containing a single row with a polygon in EPSG:4326 in a column named
                `geometry`.
            n_origin (int, optional): northing origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.
            e_origin (int, optional): easting origin to be used for the 4-byte grid ID, in EPSG:3035 (metres). Defaults to None.

        Returns:
            DataFrame: The DataFrame representing the grid tiles covering the polygon, with their grid ID and origin
                columns.
        """
        extent = self._get_polygon_sdf_extent(polygon_sdf)

        sdf = self.cover_extent_with_grid_tiles(extent, n_origin, e_origin)

        sdf = self._get_grid_intersection_with_mask(sdf, polygon_sdf)

        return sdf

    def grid_id_to_inspire_id(
        self, sdf: DataFrame, inspire_resolution: int, grid_id_col: str = None, origin: int = None
    ) -> DataFrame:
        """Function that takes a DataFrame containing 4-byte grid IDs and returns it with a new column containing
        the official INSPIRE grid ID string. Only accepted INSPIRE grid resolutions are 100m and 1km.

        It is expected that the grid ID column contains the internal representation for 100m grid tiles, and not for
        a coarser resolution. If the 100m INSPIRE grid ID was requested, the ID corresponding to the 100m grid tile
        represented by the internal grid ID is constructed. If the 1km INSPIRE grid ID was requested, the ID
        corresponding to the 1km grid tile containing the internal grid ID is constructed.

        By default, the function will use a ColNames.origin column of `sdf`. Only if the `origin` parameter is passed,
        the existence of this column will not be checked, and `origin` will be used as the origin of the 4-byte grid ID
        definition even if the column exists. This origin will be treated as an 8-byte integer, where the first (most
        significant) 4 bytes hold the easting origin divided by 100 and the last (least significant) 4 bytes hold the
        northing origin divided by 100. That is, taking the first 4 bytes and multiplying by 100 gets the easting
        value in metres (analogous for northing).

        Args:
            sdf (DataFrame): DataFrame containing the grid ID column, and a `ColNames.origin` column, to which the
                INSPIRE grid ID is to be added
            inspire_resolution (int): resolution for the INSPIRE grid ID. Currently accepts two value: `100` and `1000`.
            grid_id_col (str, optional): Name of the column containing the internal 4-byte grid ID. If None, the value
                `self.grid_id_col` is taken by default. Defaults to None
            origin (int, optional): If provided, it will be used as the origin of the definition of the 4-byte grid ID.
                It will ignore the ColNames.origin column even if it exists. If not provided, it is expected that
                `sdf` contains a ColNames.origin column, and throws an error otherwise.

        Returns:
            DataFrame: DataFrame with a new column, `ColNames.inspire_id`, containing the INSPIRE grid ID strings.

        Raises:
            ValueError: If the `inspire_resolution` is not 100 or 1000.
            ValueError: If the `origin` is not an integer.
            ValueError: If the `sdf` does not contain a ColNames.origin column and `origin` is not passed.
        """
        if grid_id_col is None:
            grid_id_col = self.grid_id_col
        if inspire_resolution not in self.ACCEPTED_RESOLUTIONS:
            raise ValueError(
                f"Expected INSPIRE resolutions are {self.ACCEPTED_RESOLUTIONS} -- received `{inspire_resolution}`"
            )
        if origin is not None:
            if not isinstance(origin, int):
                raise ValueError(f"`origin` parameter must be an integer if used -- found type {type(origin)}")
            origin_column = F.lit(origin).cast(LongType())
        else:
            if ColNames.origin not in sdf.columns:
                raise ValueError(f"`sdf` must contain a {ColNames.origin} column, or `origin` parameter must be passed")
            origin_column = F.col(ColNames.origin)

        sdf = sdf.withColumn(
            "easting",
            F.shiftrightunsigned(grid_id_col, 16).cast(LongType()) + F.shiftrightunsigned(origin_column, 32),
        ).withColumn(
            "northing",
            F.col(grid_id_col).bitwiseAND((1 << 16) - 1).cast(LongType()) + origin_column.bitwiseAND((1 << 32) - 1),
        )

        # Substract the units digit to get the ID for 1km
        if inspire_resolution == 1000:
            sdf = sdf.withColumn("northing", F.expr("northing DIV 10")).withColumn("easting", F.expr("easting DIV 10"))
        sdf = sdf.withColumn(
            ColNames.inspire_id,
            F.concat(
                F.lit(self._format_distance(inspire_resolution)),
                F.lit("N"),
                F.col("northing"),
                F.lit("E"),
                F.col("easting"),
            ),
        ).drop("northing", "easting")
        return sdf

    def grid_id_to_coarser_resolution(
        self, sdf: DataFrame, coarse_resolution: int, coarse_grid_id_col: str = None
    ) -> DataFrame:
        """This function takes a DataFrame that contains the grid ID representation of 100m grid tiles, and transforms
        it into a coarser resolution. It is always expected that the provided DataFrame has a grid ID that represents
        100m grid tiles (in the `self.grid_id_col column`), and not a different resolution.

        Notice that this method does not take into account the origin of the 4-byte grid IDs. Thus, the coarser grids
        need not be compatible with the INSPIRE definition of a resolution coarser than 100m.

        Args:
            sdf (DataFrame): DataFrame for which a coarser resolution grid ID will be computed
            coarse_resolution (int): coarser resolution to compute. Must be a multiple of `self.resolution`, i.e., 100.
            coarse_grid_id_col (str, optional): column that will hold the IDs of the grid tiles in the coarser
                resolution. If None, it will replace the original grid ID column. Defaults to None.

        Returns:
            DataFrame: DataFrame with the coarser grid IDs.
        """
        if coarse_resolution % self.resolution != 0:
            raise ValueError(f"Coarser resolution {coarse_resolution} must be a multiple of {self.resolution}")
        if coarse_resolution <= self.resolution:
            raise ValueError(f"Coarser resolution {coarse_resolution} must be greater than {self.resolution}")

        factor = coarse_resolution // self.resolution

        if coarse_grid_id_col is None:
            coarse_grid_id_col = self.grid_id_col

        sdf = sdf.withColumn("easting", F.shiftrightunsigned(ColNames.grid_id, 16)).withColumn(
            "northing", F.col(ColNames.grid_id).bitwiseAND((1 << 16) - 1)
        )

        sdf = sdf.withColumn("northing", F.col("northing") - F.col("northing") % factor).withColumn(
            "easting", F.col("easting") - F.col("easting") % factor
        )

        sdf = sdf.withColumn(coarse_grid_id_col, F.shiftleft(F.col("easting"), 16) + F.col("northing"))

        sdf = sdf.drop("northing", "easting")

        return sdf

    def grid_id_from_coarser_resolution(
        self, sdf: DataFrame, coarse_resolution: int, coarse_grid_id_col: str, new_grid_id_col: str = None
    ) -> DataFrame:
        """This function takes a DataFrame that contains the grid ID representation of grid tiles in a resolution
        coarser than 100m, and transforms it back into 100m.

        Args:
            sdf (DataFrame): DataFrame with grid IDs in a coarser resolution.
            coarse_resolution (int): coarser resolution of the grid IDs of the provided DataFrame. Must be a multiple
                of `self.resolution`, i.e., 100.
            coarse_grid_id_col (str): column that currently holds the IDs of the grid tiles in the coarser
                resolution.
            new_grid_id_col (str, optional): column that will hold the IDs of the grid tiles in the 100m resolution.
                If None, it will be set (and possible replace an existing column) as `self.grid_id_col`.
                    Defaults to None.

        Returns:
            DataFrame: DataFrame with the coarser grid IDs.
        """
        if coarse_resolution % self.resolution != 0:
            raise ValueError(f"Coarser resolution {coarse_resolution} must be a multiple of {self.resolution}")
        if coarse_resolution <= self.resolution:
            raise ValueError(f"Coarser resolution {coarse_resolution} must be greater than {self.resolution}")
        if new_grid_id_col is None:
            new_grid_id_col = self.grid_id_col

        factor = coarse_resolution // self.resolution
        offsets_df = self.spark.createDataFrame(
            [(i << 16) + j for i in range(factor) for j in range(factor)],
            schema=StructType([StructField("offset", IntegerType(), False)]),
        )

        offsets_df = F.broadcast(offsets_df)

        sdf = (
            sdf.crossJoin(offsets_df)
            .withColumn(new_grid_id_col, F.col(coarse_grid_id_col) + F.col("offset"))
            .drop("offset")
        )

        return sdf

    def inspire_id_to_grid_centroids(
        self, sdf: DataFrame, inspire_id_col: str = None, geometry_col: str = None
    ) -> DataFrame:
        """Function that takes a DataFrame containing INSPIRE grid ID strings and returns it with point geometries
        of the centroids of the corresponding grid tiles. It extracts the units and grid size from the first element
        of the DataFrame and uses it to construct the necessary geometries.

        Args:
            sdf (DataFrame): DataFrame containing the INSPIRE grid ID strings.
            inspire_id_col (str, optional): name of the column holding the INSPIRE grid IDs. If None, it is set to
                `ColNames.inspire_id`. Defaults to None.
            geometry_col (str, optional): column that will hold the grid centroid geometries. If None, it is set to
                `self.geometry`. Defaults to None.

        Returns:
            DataFrame: DataFrame with the grid centroid geometries
        """
        if inspire_id_col is None:
            inspire_id_col = ColNames.inspire_id
        if geometry_col is None:
            geometry_col = self.geometry_col

        # First, get the INSPIRE resolution
        resolution_str = sdf.select(F.regexp_extract(F.col(inspire_id_col), r"^(.*?)N", 1).alias("prefix")).first()[
            "prefix"
        ]

        # Parse and validate the INSPIRE resolution. Get the units and the grid size/resolution
        if resolution_str[-2:] == "km":
            try:
                grid_size = int(resolution_str[:-2])
            except ValueError:
                raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")
            resolution_unit = 1000
        elif resolution_str[-1:] == "m":
            try:
                grid_size = int(resolution_str[:-1])
            except ValueError:
                raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")
            resolution_unit = 100
        else:
            raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")

        # Create geometries. Multiply INSPIRE ID northing and easting values by the resolution unit, and add half
        # the grid size to get the centroid of each tile

        # Sedona has (X, Y) = (Easting, Northing) for EPSG 3035
        sdf = sdf.withColumn(
            geometry_col,
            STC.ST_Point(
                F.regexp_extract(inspire_id_col, r"E(\d+)", 1).cast(LongType()) * resolution_unit + grid_size // 2,
                F.regexp_extract(inspire_id_col, r"N(\d+)E", 1).cast(LongType()) * resolution_unit + grid_size // 2,
            ),
        )

        # Set the CRS of the geometry
        sdf = sdf.withColumn(geometry_col, STF.ST_SetSRID(geometry_col, self.GRID_CRS_EPSG_CODE))

        return sdf

    def inspire_id_to_grid_tiles(
        self, sdf: DataFrame, inspire_id_col: str = None, geometry_col: str = None
    ) -> DataFrame:
        """Function that takes a DataFrame containing INSPIRE grid ID strings and returns it with polygon geometries
        of the corresponding grid tiles. It extracts the units and grid size from the first element of the DataFrame
        and uses it to construct the necessary geometries.

        Args:
            sdf (DataFrame): DataFrame containing the INSPIRE grid ID strings.
            inspire_id_col (str, optional): name of the column holding the INSPIRE grid IDs. If None, it is set to
                `ColNames.inspire_id`. Defaults to None.
            geometry_col (str, optional): column that will hold the grid tile geometries. If None, it is set to
                `ColNames.geometry`. Defaults to None.

        Returns:
            DataFrame: DataFrame with the grid centroid geometries
        """
        if inspire_id_col is None:
            inspire_id_col = ColNames.inspire_id
        if geometry_col is None:
            geometry_col = self.geometry_col

        # First, get the INSPIRE resolution
        resolution_str = sdf.select(F.regexp_extract(F.col(inspire_id_col), r"^(.*?)N", 1).alias("prefix")).first()[
            "prefix"
        ]

        # Parse and validate the INSPIRE resolution. Get the units and the grid size/resolution
        if resolution_str[-2:] == "km":
            try:
                grid_size = int(resolution_str[:-2])
            except ValueError:
                raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")
            resolution_unit = 1000
            sdf = sdf.withColumn(
                "northing", F.regexp_extract(inspire_id_col, r"N(\d+)E", 1).cast(LongType()) * resolution_unit
            ).withColumn("easting", F.regexp_extract(inspire_id_col, r"E(\d+)", 1).cast(LongType()) * resolution_unit)
        elif resolution_str[-1:] == "m":
            try:
                grid_size = int(resolution_str[:-1])
            except ValueError:
                raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")
            resolution_unit = 1
            sdf = sdf.withColumn(
                "northing", F.regexp_extract(inspire_id_col, r"N(\d+)E", 1).cast(LongType()) * grid_size
            ).withColumn("easting", F.regexp_extract(inspire_id_col, r"E(\d+)", 1).cast(LongType()) * grid_size)
        else:
            raise ValueError(f"Unexpected INSPIRE grid resolution string `{resolution_str}`")

        # Sedona has (X, Y) = (Easting, Northing) for EPSG 3035
        sdf = sdf.withColumn(
            geometry_col,
            STC.ST_PolygonFromEnvelope(
                F.col("easting"),  # min_x (min_easting)
                F.col("northing"),  # min_y, (min_northing)
                F.col("easting") + F.lit(resolution_unit * grid_size),  # max_x (max_easting)
                F.col("northing") + F.lit(resolution_unit * grid_size),  # max_y (max_northing)
            ),
        )

        sdf = sdf.drop("northing", "easting")

        # Set the CRS of the geometry
        sdf = sdf.withColumn(geometry_col, STF.ST_SetSRID(geometry_col, self.GRID_CRS_EPSG_CODE))

        return sdf

    def grid_ids_to_grid_centroids(
        self,
        sdf: DataFrame,
        grid_resolution: int,
        grid_id_col: str = None,
        geometry_col: str = None,
        origin: int = None,
    ) -> DataFrame:
        """Function that takes a DataFrame containing internal 4-byte grid IDs and returns it with point geometries
        of the centroids of the corresponding grid tiles.

        By default, the function will use a ColNames.origin column of `sdf`. Only if the `origin` parameter is passed,
        the existence of this column will not be checked, and `origin` will be used as the origin of the 4-byte grid ID
        definition even if the column exists. This origin will be treated as an 8-byte integer, where the first (most
        significant) 4 bytes hold the easting origin divided by 100 and the last (least significant) 4 bytes hold the
        northing origin divided by 100. That is, taking the first 4 bytes and multiplying by 100 gets the easting
        value in metres (analogous for northing).

        Args:
            sdf (DataFrame): DataFrame containing the internal 4-byte grid IDs.
            grid_resolution (int): resolution, in metres, of the current grid as represented by the internal 4-byte
                grid IDs. Must be a multiple of `self.resolution`. i.e. 100.
            grid_id_col (str, optional): column that holds the internal grid IDs. If None, it is set to
                `self.grid_id_col`. Defaults to None.
            geometry_col (str, optional): column that will hold the grid centroid geometries. If None, it is set to
                `self.geometry_col`. Defaults to None.
            origin (int, optional): If provided, it will be used as the origin of the definition of the 4-byte grid ID.
                It will ignore the ColNames.origin column even if it exists. If not provided, it is expected that
                `sdf` contains a ColNames.origin column, and throws an error otherwise.

        Returns:
            DataFrame: DataFrame with the grid centroid geometries
        """

        if grid_resolution % self.resolution != 0:
            raise ValueError(f"Grid resolution must be a multiple of {self.resolution}")
        if geometry_col is None:
            geometry_col = self.geometry_col
        if grid_id_col is None:
            grid_id_col = self.grid_id_col
        if origin is not None:
            if not isinstance(origin, int):
                raise ValueError(f"`origin` parameter must be an integer if used -- found type {type(origin)}")
            origin_column = F.lit(origin).cast(LongType())
        else:
            if ColNames.origin not in sdf.columns:
                raise ValueError(f"`sdf` must contain a {ColNames.origin} column, or `origin` parameter must be passed")
            origin_column = F.col(ColNames.origin)

        # For Sedona, (X, Y) == (Easting, Northing) in EPSG 3035
        sdf = sdf.withColumn(
            geometry_col,
            STC.ST_Point(
                (F.shiftrightunsigned(ColNames.grid_id, 16).cast(LongType()) + F.shiftrightunsigned(origin_column, 32))
                * self.resolution
                + grid_resolution // 2,
                (
                    F.col(ColNames.grid_id).bitwiseAND((1 << 16) - 1).cast(LongType())
                    + origin_column.bitwiseAND((1 << 32) - 1)
                )
                * self.resolution
                + grid_resolution // 2,
            ),
        )

        sdf = sdf.withColumn(geometry_col, STF.ST_SetSRID(geometry_col, self.GRID_CRS_EPSG_CODE))

        return sdf

    def grid_ids_to_grid_tiles(
        self, sdf: DataFrame, grid_resolution: int, geometry_col: str = None, origin: int = None
    ) -> DataFrame:
        """Function that takes a DataFrame containing internal 4-byte grid IDs and returns it with polygon geometries
        of the corresponding grid tiles.

        By default, the function will use a ColNames.origin column of `sdf`. Only if the `origin` parameter is passed,
        the existence of this column will not be checked, and `origin` will be used as the origin of the 4-byte grid ID
        definition even if the column exists. This origin will be treated as an 8-byte integer, where the first (most
        significant) 4 bytes hold the easting origin divided by 100 and the last (least significant) 4 bytes hold the
        northing origin divided by 100. That is, taking the first 4 bytes and multiplying by 100 gets the easting
        value in metres (analogous for northing).

        Args:
            sdf (DataFrame): DataFrame containing the internal 4-byte grid IDs.
            grid_resolution (int): resolution, in metres, of the current grid as represented by the internal 4-byte
                grid IDs. Must be a multiple of `self.resolution`. i.e. 100.
            geometry_col (str, optional): column that will hold the grid tile geometries. If None, it is set to
                `self.geometry_col`. Defaults to None.
            origin (int, optional): If provided, it will be used as the origin of the definition of the 4-byte grid ID.
                It will ignore the ColNames.origin column even if it exists. If not provided, it is expected that
                `sdf` contains a ColNames.origin column, and throws an error otherwise.

        Returns:
            DataFrame: DataFrame with the grid centroid geometries
        """
        if grid_resolution % self.resolution != 0:
            raise ValueError(f"Grid resolution must be a multiple of {self.resolution}")
        if geometry_col is None:
            geometry_col = self.geometry_col
        if origin is not None:
            if not isinstance(origin, int):
                raise ValueError(f"`origin` parameter must be an integer if used -- found type {type(origin)}")
            origin_column = F.lit(origin).cast(LongType())
        else:
            if ColNames.origin not in sdf.columns:
                raise ValueError(f"`sdf` must contain a {ColNames.origin} column, or `origin` parameter must be passed")
            origin_column = F.col(ColNames.origin)

        sdf = sdf.withColumn(
            "easting",
            (F.shiftrightunsigned(ColNames.grid_id, 16).cast(LongType()) + F.shiftrightunsigned(origin_column, 32))
            * self.resolution,
        ).withColumn(
            "northing",
            (
                F.col(ColNames.grid_id).bitwiseAND((1 << 16) - 1).cast(LongType())
                + origin_column.bitwiseAND((1 << 32) - 1)
            )
            * self.resolution,
        )

        # For Sedona, (X, Y) == (Easting, Northing) in EPSG 3035
        sdf = sdf.withColumn(
            geometry_col,
            STC.ST_PolygonFromEnvelope(
                F.col("easting"),  # min_x (min_easting)
                F.col("northing"),  # min_y (min_northing)
                F.col("easting") + grid_resolution,  # max_x (max_easting)
                F.col("northing") + grid_resolution,  # max_y (max_northing)
            ),
        )

        sdf = sdf.drop("northing", "easting")

        # Set the CRS of the geometries
        sdf = sdf.withColumn(geometry_col, STF.ST_SetSRID(geometry_col, self.GRID_CRS_EPSG_CODE))

        return sdf
