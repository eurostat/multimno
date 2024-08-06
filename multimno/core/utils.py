"""
This module contains utility functions for the multimno package.
"""

import math
from typing import List, Tuple
import geopandas as gpd

from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP
from sedona.sql import st_aggregates as STA
from sedona.sql import st_functions as STF
from sedona.sql.types import GeometryType

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType, BooleanType
from multimno.core.constants.columns import ColNames


def quadkey_to_extent(quadkey: str):
    """
    Converts a quadkey to a geographic extent (bounding box).

    This function takes a quadkey and converts it to a geographic extent represented as a tuple of
    (longitude_min, latitude_min, longitude_max, latitude_max).

    Args:
        quadkey (str): The quadkey to convert. A quadkey is a string of digits that represents a
        specific tile in a quadtree-based spatial index.

    Returns:
        tuple: A tuple representing the geographic extent of the quadkey. The tuple contains four
        elements: (longitude_min, latitude_min, longitude_max, latitude_max).
    """
    tile_x, tile_y, zoom_level = quadkey_to_tile(quadkey)
    n = 2.0**zoom_level
    lon_min = tile_x / n * 360.0 - 180.0
    lat_min = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (tile_y + 1) / n))))
    lon_max = (tile_x + 1) / n * 360.0 - 180.0
    lat_max = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n))))

    return (lon_min, lat_min, lon_max, lat_max)


def quadkey_to_tile(quadkey: str) -> Tuple[int, int, int]:
    """
    Converts a quadkey to tile coordinates and zoom level.

    This function takes a quadkey and converts it to tile coordinates (tile_x, tile_y) and zoom level.
    A quadkey is a string of digits that represents a specific tile in a quadtree-based spatial index.

    Args:
        quadkey (str): The quadkey to convert.

    Returns:
        tuple: A tuple representing the tile coordinates and zoom level of the quadkey. The tuple contains three
        elements: (tile_x, tile_y, zoom_level).

    Raises:
        ValueError: If the quadkey contains an invalid character.
    """
    tile_x = tile_y = 0
    zoom_level = len(quadkey)
    for i in range(zoom_level):
        bit = zoom_level - i - 1
        mask = 1 << bit
        if quadkey[i] == "0":
            pass
        elif quadkey[i] == "1":
            tile_x |= mask
        elif quadkey[i] == "2":
            tile_y |= mask
        elif quadkey[i] == "3":
            tile_x |= mask
            tile_y |= mask
        else:
            raise ValueError("Invalid quadkey character.")
    return tile_x, tile_y, zoom_level


def latlon_to_tilexy(latitude: float, longitude: float, level_of_detail: int) -> Tuple[int, int]:
    """
    Converts a geographic coordinate to tile coordinates at a specific zoom level.

    This function takes a latitude and longitude in degrees, and a zoom level, and converts them to
    tile coordinates (tile_x, tile_y) at the specified zoom level. The tile coordinates are in the
    tile system used by Bing Maps, OpenStreetMap, MapBox and other map providers.

    Args:
        latitude (float): The latitude of the geographic coordinate, in degrees.
        longitude (float): The longitude of the geographic coordinate, in degrees.
        level_of_detail (int): The zoom level.

    Returns:
        tuple: A tuple representing the tile coordinates of the geographic coordinate at the specified
        zoom level. The tuple contains two elements: (tile_x, tile_y).
    """
    if not -90 <= latitude <= 90:
        raise ValueError(f"Latitude must be in the range [-90, 90], got {latitude}")
    if not -180 <= longitude <= 180:
        raise ValueError(f"Longitude must be in the range [-180, 180], got {longitude}")
    latitude = math.radians(latitude)
    longitude = math.radians(longitude)

    sinLatitude = math.sin(latitude)
    pixelX = ((longitude + math.pi) / (2 * math.pi)) * 256 * 2**level_of_detail
    pixelY = (0.5 - math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * math.pi)) * 256 * 2**level_of_detail
    tileX = int(math.floor(pixelX / 256))
    tileY = int(math.floor(pixelY / 256))
    return tileX, tileY


def tilexy_to_quadkey(x: int, y: int, level_of_detail: int) -> str:
    """
    Converts tile coordinates to a quadkey at a specific zoom level.

    This function takes tile coordinates (x, y) and a zoom level, and converts them to a quadkey.
    The quadkey is a string of digits that represents a specific tile in a quadtree-based spatial index.
    The conversion process involves bitwise operations on the tile coordinates.

    Args:
        x (int): The x-coordinate of the tile.
        y (int): The y-coordinate of the tile.
        level_of_detail (int): The zoom level.

    Returns:
        str: The quadkey representing the tile at the specified zoom level.
    """
    quadkey = ""
    for i in range(level_of_detail, 0, -1):
        digit = 0
        mask = 1 << (i - 1)
        if (x & mask) != 0:
            digit += 1
        if (y & mask) != 0:
            digit += 2
        quadkey += str(digit)
    return quadkey


def latlon_to_quadkey(latitude: float, longitude: float, level_of_detail: int) -> str:
    """
    Converts a geographic coordinate to a quadkey at a specific zoom level.

    This function takes a latitude and longitude in degrees, and a zoom level, and converts them to a quadkey.
    The quadkey is a string of digits that represents a specific tile in a quadtree-based spatial index.
    The conversion process involves first converting the geographic coordinate to tile coordinates,
    and then converting the tile coordinates to a quadkey.

    Args:
        latitude (float): The latitude of the geographic coordinate, in degrees.
        longitude (float): The longitude of the geographic coordinate, in degrees.
        level_of_detail (int): The zoom level.

    Returns:
        str: The quadkey representing the geographic coordinate at the specified zoom level.
    """
    x, y = latlon_to_tilexy(latitude, longitude, level_of_detail)
    return tilexy_to_quadkey(x, y, level_of_detail)


def get_quadkeys_for_bbox(extent: Tuple[float, float, float, float], level_of_detail: int) -> List[str]:
    """
    Generates a list of quadkeys for a bounding box at a specific zoom level.

    This function takes a bounding box defined by its lon min, lat min, lon max, and lat max extents,
    and a zoom level, and generates a list of quadkeys that cover the bounding box at the specified zoom level.
    The quadkeys are strings of digits that represent specific tiles in a quadtree-based spatial index.

    Args:
        extent (tuple): A tuple representing the bounding box. The tuple contains four elements:
            (west, south, east, north), which are the western, southern, eastern, and northern extents
            of the bounding box, respectively. Each extent is a float representing a geographic coordinate in degrees.
        level_of_detail (int): The zoom level.

    Returns:
        list: A list of quadkeys that cover the bounding box at the specified zoom level. Each quadkey is a string.
    """
    west, south, east, north = extent
    min_tile_x, min_tile_y = latlon_to_tilexy(north, west, level_of_detail)
    max_tile_x, max_tile_y = latlon_to_tilexy(south, east, level_of_detail)
    quadkeys = []
    for x in range(min_tile_x, max_tile_x + 1):
        for y in range(min_tile_y, max_tile_y + 1):
            quadkeys.append(tilexy_to_quadkey(x, y, level_of_detail))
    return quadkeys


def assign_quadkey(sdf: DataFrame, crs_in: int, zoom_level: int) -> DataFrame:
    """
    Assigns a quadkey to each row in a DataFrame based on the centroid of its geometry.

    Args:
        sdf (DataFrame): The DataFrame to assign quadkeys to. The DataFrame must contain a geometry column.
        crs_in (int): The CRS of the dataframe to project to 4326 before assigning quadkeys.
        zoom_level (int): The zoom level to use when assigning quadkeys.

    Returns:
        DataFrame: A DataFrame containing the same rows as the input DataFrame, but with an additional quadkey column.
    """

    quadkey_udf = F.udf(latlon_to_quadkey, StringType())
    sdf = sdf.withColumn("centroid", STF.ST_Centroid(ColNames.geometry))

    if crs_in != 4326:
        sdf = project_to_crs(sdf, crs_in, 4326, "centroid")

    sdf = sdf.withColumn(
        "quadkey",
        quadkey_udf(
            STF.ST_Y(F.col("centroid")),
            STF.ST_X(F.col("centroid")),
            F.lit(zoom_level),
        ),
    ).drop("centroid")

    return sdf


# TODO: make this function work properly
def coarsen_quadkey_to_partition_size(sdf, desired_partition_size, minimum_quadkey_length):

    cells_per_quad = desired_partition_size / 4
    continue_coarsening = True

    while continue_coarsening:
        # Count elements per quadkey
        windowSpec = Window.partitionBy("quadkey")
        sdf = sdf.withColumn("count", F.count("*").over(windowSpec))

        # Check if further coarsening is possible
        coarsening_needed = sdf.filter(
            (F.col("count") < cells_per_quad) & (F.length("quadkey") > minimum_quadkey_length)
        )

        # Exit the loop if no more coarsening is needed
        if coarsening_needed.count() == 0:
            continue_coarsening = False
        else:
            # Coarsen quadkeys by reducing their length
            sdf = sdf.withColumn(
                "quadkey",
                F.when(
                    (F.col("count") < cells_per_quad) & (F.length("quadkey") > minimum_quadkey_length),
                    F.expr("substring(quadkey, 1, length(quadkey) - 1)"),
                ).otherwise(F.col("quadkey")),
            )
            sdf = sdf.drop("count")
            sdf = sdf.cache()  # Re-cache the DataFrame
            sdf.count()  # Trigger the cache with an action

    return sdf.drop("count")


def fix_geometry(sdf: DataFrame, geometry_type: int, geometry_column: str = "geometry") -> DataFrame:
    """
    Fixes the geometry of a given type in a DataFrame.
    This function applies several operations to the geometries in the specified geometry column of the DataFrame:
    1. If a geometry is a collection of geometries, extracts only the geometries of the given type.
    2. Filters out any geometries of type other than given.
    3. Removes any invalid geometries.
    4. Removes any empty geometries.
    Args:
        sdf (DataFrame): The DataFrame containing the geometries to check.
        geometry_column (str, optional): The name of the column containing the geometries. Defaults to "geometry".
    Returns:
        DataFrame: The DataFrame with the fixed polygon geometries.
    """
    geometry_name = "Polygon" if geometry_type == 3 else ("Line" if geometry_type == 2 else "Point")
    if geometry_type == 3:
        sdf = sdf.withColumn(geometry_column, STF.ST_ReducePrecision(F.col(geometry_column), F.lit(4)))
    sdf = (
        sdf.withColumn(
            geometry_column,
            F.when(
                STF.ST_IsCollection(F.col(geometry_column)),
                STF.ST_CollectionExtract(geometry_column, F.lit(geometry_type)),
            ).otherwise(F.col(geometry_column)),
        )
        .filter(~STF.ST_IsEmpty(F.col(geometry_column)))
        .filter(STF.ST_GeometryType(F.col(geometry_column)).like(f"%{geometry_name}%"))
        .filter(STF.ST_IsValid(geometry_column))
    )
    return sdf


def cut_polygons_with_mask_polygons(
    input_sdf: DataFrame,
    mask_sdf: DataFrame,
    cols_to_keep: List[str],
    self_intersection=False,
    geometry_column: str = "geometry",
) -> DataFrame:
    """
    Cuts polygons in the input DataFrame with mask polygons from another DataFrame.
    This function takes two DataFrames: one with input polygons and another with mask polygons.
    It cuts the input polygons with the mask polygons, and returns a new DataFrame with the resulting polygons.
    Both dataframes have to have same coordinate system.
    Args:
        input_sdf (DataFrame): A DataFrame containing the input polygons.
        mask_sdf (DataFrame): A DataFrame containing the mask polygons.
        cols_to_keep (list): A list of column names to keep from the input DataFrame.
        geometry_column (str, optional): The name of the geometry column in the DataFrames.
            Defaults to "geometry".
    Returns:
        DataFrame: A DataFrame containing the resulting polygons after cutting the input polygons with the mask polygons.
    """
    input_sdf = input_sdf.withColumn("id", F.monotonically_increasing_id())
    cols_to_keep = [f"a.{col}" for col in cols_to_keep]
    if self_intersection:
        input_sdf = input_sdf.withColumn("area", STF.ST_Area(geometry_column))
        intersection = input_sdf.alias("a").join(
            input_sdf.alias("b"),
            STP.ST_Intersects("a.geometry", "b.geometry") & (F.col("a.area") > F.col("b.area")),
        )
        input_sdf = input_sdf.drop("area")
    else:
        intersection = input_sdf.alias("a").join(
            mask_sdf.alias("b"),
            STP.ST_Intersects("a.geometry", "b.geometry"),
        )
    intersection_cut = intersection.groupby("a.id", *cols_to_keep).agg(
        STA.ST_Union_Aggr(f"b.{geometry_column}").alias("cut_geometry")
    )
    intersection_cut = fix_geometry(intersection_cut, 3, "cut_geometry")
    intersection_cut = intersection_cut.withColumn(
        geometry_column, STF.ST_Difference(f"a.{geometry_column}", "cut_geometry")
    ).drop("cut_geometry")

    non_intersection = input_sdf.join(intersection_cut, ["id"], "left_anti")

    return non_intersection.union(intersection_cut).drop("id")


def filter_geodata_to_extent(
    sdf: DataFrame,
    extent: Tuple[float, float, float, float],
    target_crs: int,
    geometry_column: str = "geometry",
) -> DataFrame:
    """
    Filters a DataFrame to include only rows with geometries that intersect a specified extent.

    Args:
        sdf (DataFrame): The DataFrame to filter. The DataFrame must contain a geometry column.
        extent (tuple): A tuple representing the extent. The tuple contains four elements:
            (west, south, east, north), which are the western, southern, eastern, and northern bounds of the WGS84 extent.
        target_crs (int): The CRS of DataFrame to transform the extent to.
        geometry_column (str, optional): The name of the geometry column. Defaults to "geometry".

    Returns:
        DataFrame: A DataFrame containing only the rows from the input DataFrame where the geometry intersects the extent.
    """

    extent = STC.ST_PolygonFromEnvelope(*extent)
    if target_crs != 4326:
        extent = STF.ST_Transform(extent, F.lit("EPSG:4326"), F.lit(f"EPSG:{target_crs}"))

    sdf = sdf.filter(STP.ST_Intersects(extent, F.col(geometry_column)))

    return sdf


def cut_geodata_to_extent(
    sdf: DataFrame,
    extent: Tuple[float, float, float, float],
    target_crs: int,
    geometry_column: str = "geometry",
) -> DataFrame:
    """
    Cuts geometries in a DataFrame to a specified extent.

    Args:
        sdf (DataFrame): The DataFrame to filter. The DataFrame must contain a geometry column.
        extent (tuple): A tuple representing the extent. The tuple contains four elements:
            (west, south, east, north), which are the western, southern, eastern, and northern bounds of the WGS84 extent.
        target_crs (int): The CRS of DataFrame to transform the extent to.
        geometry_column (str, optional): The name of the geometry column. Defaults to "geometry".

    Returns:
        DataFrame: A DataFrame containing the same rows as the input DataFrame, but with the geometries cut to the extent.
    """

    extent = STC.ST_PolygonFromEnvelope(*extent)
    if target_crs != 4326:
        extent = STF.ST_Transform(extent, F.lit("EPSG:4326"), F.lit(f"EPSG:{target_crs}"))

    sdf = sdf.withColumn(geometry_column, STF.ST_Intersection(F.col(geometry_column), extent))

    return sdf


def project_to_crs(sdf: DataFrame, crs_in: int, crs_out: int, geometry_column="geometry") -> DataFrame:
    """
    Projects geometry to CRS.

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
        geometry_column,
        STF.ST_Transform(sdf[geometry_column], F.lit(crs_in), F.lit(crs_out)),
    )
    return sdf


def get_epsg_from_geometry_column(df: DataFrame) -> int:
    """
    Get the EPSG code from the geometry column of a DataFrame.

    Args:
        df (DataFrame): DataFrame with a geometry column.

    Raises:
        ValueError: If the DataFrame contains multiple EPSG codes.

    Returns:
        int: EPSG code of the geometry column.
    """
    # Get the EPSG code from the geometry column
    temp = df.select(STF.ST_SRID("geometry")).distinct().persist()
    if temp.count() > 1:
        raise ValueError("Dataframe contains multiple EPSG codes")

    epsg = temp.collect()[0][0]
    return epsg


def spark_to_geopandas(df: DataFrame, epsg: int = None) -> gpd.GeoDataFrame:
    """
    Convert a Spark DataFrame to a geopandas GeoDataFrame.

    Args:
        df (DataFrame): Spark DataFrame to convert.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with the same data as the input DataFrame.
    """
    # Convert the DataFrame to a GeoDataFrame
    if epsg is None:
        epsg = get_epsg_from_geometry_column(df)
    gdf = gpd.GeoDataFrame(df.toPandas(), crs=f"EPSG:{epsg}")

    return gdf


def calc_hashed_user_id(df: DataFrame, user_column: str = ColNames.user_id) -> DataFrame:
    """
    Calculates SHA2 hash of user id, takes the first 31 bits and converts them to a non-negative 32-bit integer.

    Args:
        df (pyspark.sql.DataFrame): Data of clean synthetic events with a user id column.

    Returns:
        pyspark.sql.DataFrame: Dataframe, where user_id column is transformered to a hashed value.

    """

    df = df.withColumn(user_column, F.unhex(F.sha2(F.col(user_column).cast("string"), 256)))
    return df


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
