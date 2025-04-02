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

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType
from multimno.core.constants.columns import ColNames


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
        .withColumn(geometry_column, STF.ST_ReducePrecision(F.col(geometry_column), F.lit(5)))
    )
    return sdf


def merge_geom_within_mask_geom(
    input_sdf: DataFrame, mask_sdf: DataFrame, cols_to_keep: List, geometry_col: str
) -> DataFrame:
    """
    Merges geometries from an input DataFrame that intersect with geometries from a mask DataFrame.

    This function performs a spatial join between input and mask DataFrames using ST_Intersects,
    calculates the geometric intersection between each matching pair of geometries,
    then groups by specified columns and unions the resulting intersection geometries.

    Args:
        input_sdf (DataFrame): Input DataFrame containing geometries to be processed.
                              Must contain a 'geometry' column.
        mask_sdf (DataFrame): Mask DataFrame containing geometries that define the areas of interest.
                              Must contain a 'geometry' column.
        cols_to_keep (List): List of column names from the input DataFrame to preserve in the output.
                            These columns will be used as grouping keys.

    Returns:
        DataFrame: A DataFrame containing merged geometries that result from intersecting the input
                  geometries with the mask geometries.
    """

    merge_sdf = (
        input_sdf.alias("a")
        .join(
            mask_sdf.alias("b"),
            STP.ST_Intersects(f"a.{geometry_col}", f"b.{geometry_col}"),
        )
        .withColumn("merge_geometry", STF.ST_Intersection(f"a.{geometry_col}", f"b.{geometry_col}"))
        .groupBy(*cols_to_keep)
        .agg(F.array_agg("merge_geometry").alias(geometry_col))
        .withColumn(geometry_col, F.explode(STF.ST_Dump(STF.ST_Union(geometry_col))))
    )

    return merge_sdf


def clip_polygons_with_mask_polygons(
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
        # Join smaller polygons to larger polygons
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
    intersection_cut = (
        intersection.groupby("a.id", *cols_to_keep)
        .agg(F.array_agg(f"b.{geometry_column}").alias("cut_geometry"))
        .withColumn("cut_geometry", STF.ST_Union("cut_geometry"))
    )
    intersection_cut = fix_geometry(intersection_cut, 3, "cut_geometry")
    intersection_cut = intersection_cut.withColumn(
        geometry_column, STF.ST_Difference(f"a.{geometry_column}", "cut_geometry")
    ).drop("cut_geometry")

    non_intersection = input_sdf.join(intersection_cut, ["id"], "left_anti")
    full_sdf = non_intersection.union(intersection_cut).drop("id")

    full_sdf = fix_geometry(full_sdf, 3, geometry_column)

    return full_sdf


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

    sdf = filter_geodata_to_extent(sdf, extent, target_crs, geometry_column)

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
