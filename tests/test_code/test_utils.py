import os
import shutil

from geopandas.testing import assert_geodataframe_equal
from sedona.sql import st_functions as STF
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.testing.utils import assertDataFrameEqual

from multimno.core.utils import spark_to_geopandas
from tests.test_code.test_common import TESTING_DATA_PATH


# ASSERT geodataframe equal


def assert_sparkgeodataframe_equal(df_left: DataFrame, df_right: DataFrame):
    df_left = df_left.withColumn("geometry", STF.ST_AsEWKB(F.col("geometry")))
    df_right = df_right.withColumn("geometry", STF.ST_AsEWKB(F.col("geometry")))
    assertDataFrameEqual(df_left, df_right)


def assert_geodataframe_almost_equal(df_left, df_right, precision=0.0001):
    assertDataFrameEqual(df_left.drop("geometry"), df_right.drop("geometry"))
    df_left = spark_to_geopandas(df_left.select("geometry")).to_crs("EPSG:4326")
    df_right = spark_to_geopandas(df_right.select("geometry")).to_crs("EPSG:4326")

    assert df_right["geometry"].geom_equals_exact(df_left["geometry"], precision).all()


def assert_geometry_deep_almost_equal(df_left, df_right, precision=0.0001):
    df_left = spark_to_geopandas(df_left.select("geometry"))
    df_right = spark_to_geopandas(df_right.select("geometry"))

    # Get geometry type
    left_geom_type = df_left.geom_type
    right_geom_type = df_right.geom_type

    if left_geom_type != right_geom_type:
        raise ValueError(f"Geometry type mismatch: {left_geom_type} != {right_geom_type}")

    # TODO: geometry deep comparison
    # Compare Point geometry

    # Compare Multi-Point geometry (sort the coordinates)

    # Compare LineString geometry (use a buffer to compare)

    # Compare Multi-LineString geometry (use a buffer to compare)

    # Compare Polygon geometry (symmetric difference)
    # assert

    # Compare Multi-Polygon geometry (symmetric difference)

    raise NotImplementedError("TODO: deep comparison")


# Testing data directory setup


def setup_test_data_dir():
    shutil.rmtree(TESTING_DATA_PATH, ignore_errors=True)
    os.makedirs(TESTING_DATA_PATH, exist_ok=True)


def teardown_test_data_dir():
    shutil.rmtree(TESTING_DATA_PATH, ignore_errors=True)
