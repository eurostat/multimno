"""
This module contains utility functions to work with quadkey geo indexing for the multimno package.
"""

import math
from typing import List, Tuple

from sedona.sql import st_functions as STF
from sedona.sql import st_functions as STF
from sedona.sql import st_constructors as STC

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from multimno.core.constants.columns import ColNames
from multimno.core import utils


def quadkeys_to_extent_dataframe(spark, quadkeys: List[str], crs: str = 4326) -> DataFrame:
    """
    Converts a list of quadkeys to a Spark DataFrame with geometry polygons representing their extents.

    This function takes a list of quadkeys and creates a DataFrame where each row contains
    a quadkey and its corresponding geometry (polygon) based on the geographic extent.
    Uses ST_PolygonFromEnvelope for efficient polygon creation.

    Args:
        spark: The Spark session
        quadkeys (List[str]): List of quadkeys to convert to geometries
        crs (str): Coordinate reference system for the output geometries, defaults to WGS84

    Returns:
        DataFrame: A Spark DataFrame with columns 'quadkey' and 'geometry'
    """

    # Create rows with quadkey and extent coordinates
    data = []
    for quadkey in quadkeys:
        lon_min, lat_min, lon_max, lat_max = quadkey_to_extent(quadkey)
        data.append((quadkey, [lon_min, lat_min, lon_max, lat_max]))

    # Create DataFrame schema
    schema = StructType(
        [StructField("quadkey", StringType(), False), StructField("extent", ArrayType(DoubleType()), False)]
    )

    # Create DataFrame from data and schema
    df = spark.createDataFrame(data, schema)

    # Convert extent arrays to Sedona geometries using ST_PolygonFromEnvelope
    df = df.withColumn(
        "geometry",
        STC.ST_PolygonFromEnvelope(
            F.col("extent")[0],  # xmin
            F.col("extent")[1],  # ymin
            F.col("extent")[2],  # xmax
            F.col("extent")[3],  # ymax
        ),
    )

    # Transform to the desired CRS if needed
    if crs != 4326:
        df = df.withColumn("geometry", STF.ST_Transform("geometry", F.lit("epsg:4326"), F.lit(f"epsg:{crs}")))

    # Return the DataFrame with quadkey and geometry columns, dropping the intermediate array
    return df.select("quadkey", "geometry")


def quadkey_to_extent(quadkey: str) -> Tuple[float, float, float, float]:
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


def get_children_quadkeys(quadkey: str, target_level: int) -> List[str]:
    """
    Generates all child quadkeys at a specified resolution level for a given quadkey.

    This function takes a parent quadkey and a target level of detail, then returns
    all child quadkeys that are contained within the parent quadkey's area at the
    specified target level.

    Args:
        quadkey (str): The parent quadkey.
        target_level (int): The target level of detail (zoom level) for the child quadkeys.

    Returns:
        List[str]: A list of all child quadkeys at the specified target level.

    Raises:
        ValueError: If target_level is less than the level of the input quadkey.
    """
    # Get the level of the input quadkey
    current_level = len(quadkey)

    # Check that target_level is greater than or equal to current_level
    if target_level < current_level:
        raise ValueError(
            f"Target level ({target_level}) must be greater than or equal to the current level ({current_level})"
        )

    # If target_level is the same as current_level, return the input quadkey
    if target_level == current_level:
        return [quadkey]

    # Initialize the list of child quadkeys with the input quadkey
    child_quadkeys = [quadkey]

    # For each level between current_level and target_level
    for _ in range(target_level - current_level):
        # Initialize a new list to hold the next level of child quadkeys
        next_level_quadkeys = []

        # For each quadkey in the current list
        for qk in child_quadkeys:
            # Generate the four children of this quadkey
            next_level_quadkeys.extend([qk + "0", qk + "1", qk + "2", qk + "3"])

        # Update the list of child quadkeys
        child_quadkeys = next_level_quadkeys

    return child_quadkeys


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
        sdf = utils.project_to_crs(sdf, crs_in, 4326, "centroid")

    sdf = sdf.withColumn(
        "quadkey",
        quadkey_udf(
            STF.ST_Y(F.col("centroid")),
            STF.ST_X(F.col("centroid")),
            F.lit(zoom_level),
        ),
    ).drop("centroid")

    return sdf
