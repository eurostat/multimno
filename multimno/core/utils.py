"""
This module contains utility functions for the multimno package.
"""

import pyspark.sql.functions as F
import geopandas as gpd
from sedona.sql import st_functions as STF
from pyspark.sql import DataFrame
from multimno.core.constants.columns import ColNames


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
