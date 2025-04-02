import pytest
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.testing.utils import assertDataFrameEqual
import pyspark.sql.functions as psf
from sedona.sql import st_constructors as STC
from multimno.core.grid import InspireGridGenerator

from tests.test_code.fixtures import spark_session as spark
from tests.test_code.test_utils import assert_geodataframe_almost_equal, assert_sparkgeodataframe_equal

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture(scope="module")
def grid_generator(spark):
    # Use default values
    return InspireGridGenerator(spark, resolution=100, grid_partition_size=500)


@pytest.fixture(scope="module")
def centroid_grid(spark):
    data = [
        Row(geometry="POINT (3157850 2031050)", grid_id="20310003157800"),
        Row(geometry="POINT (3157950 2031050)", grid_id="20310003157900"),
        Row(geometry="POINT (3158050 2031050)", grid_id="20310003158000"),
        Row(geometry="POINT (3158150 2031050)", grid_id="20310003158100"),
        Row(geometry="POINT (3158250 2031050)", grid_id="20310003158200"),
    ]
    df = spark.createDataFrame(data)
    df = df.withColumn(
        "geometry",
        STC.ST_GeomFromEWKT(
            psf.concat(psf.lit(f"SRID={InspireGridGenerator.GRID_CRS_EPSG_CODE};"), psf.col("geometry"))
        ),
    )
    df = df.select("grid_id", "geometry")

    return df


@pytest.fixture(scope="module")
def tile_grid(spark):
    data = [
        Row(
            geometry="POLYGON ((3157800 2031100, 3157900 2031100, 3157900 2031000, 3157800 2031000, 3157800 2031100))",
            grid_id="20310003157800",
        ),
        Row(
            geometry="POLYGON ((3157900 2031100, 3158000 2031100, 3158000 2031000, 3157900 2031000, 3157900 2031100))",
            grid_id="20310003157900",
        ),
        Row(
            geometry="POLYGON ((3158000 2031100, 3158100 2031100, 3158100 2031000, 3158000 2031000, 3158000 2031100))",
            grid_id="20310003158000",
        ),
        Row(
            geometry="POLYGON ((3158100 2031100, 3158200 2031100, 3158200 2031000, 3158100 2031000, 3158100 2031100))",
            grid_id="20310003158100",
        ),
        Row(
            geometry="POLYGON ((3158200 2031100, 3158300 2031100, 3158300 2031000, 3158200 2031000, 3158200 2031100))",
            grid_id="20310003158200",
        ),
    ]
    df = spark.createDataFrame(data)
    df = df.withColumn(
        "geometry",
        STC.ST_GeomFromEWKT(
            psf.concat(psf.lit(f"SRID={InspireGridGenerator.GRID_CRS_EPSG_CODE};"), psf.col("geometry"))
        ),
    )
    df = df.select("grid_id", "geometry")

    return df


@pytest.mark.skip(reason="TODO: New Grid implementation")
def test_cover_extent_with_grid_centroids(grid_generator):
    # Define the extent of the polygon
    extent = [-3.715, 40.410, -3.694, 40.425]

    # Generate the grid centroids covering the extent
    result = grid_generator.cover_extent_with_grid_centroids(extent)

    # Assert that the result is a DataFrame
    assert isinstance(result, DataFrame)

    # Assert that the result DataFrame has the expected columns
    expected_columns = ["geometry", "grid_id"]
    assert result.columns == expected_columns

    # Assert that the result DataFrame
    assert result.count() == 713


@pytest.mark.skip(reason="TODO: New Grid implementation")
def test_cover_extent_with_grid_cells(grid_generator):
    # Define the extent of the polygon
    extent = [-3.715, 40.410, -3.694, 40.425]

    # Generate the grid cells covering the extent
    result = grid_generator.cover_extent_with_grid_ids(extent)

    # Assert that the result is a DataFrame
    assert isinstance(result, DataFrame)

    # Assert that the result DataFrame has the expected columns
    expected_columns = ["grid_id"]
    assert result.columns == expected_columns

    # Assert that the result DataFrame
    assert result.count() == 713


@pytest.mark.skip(reason="TODO: New Grid implementation")
def test_cover_extent_with_grid_tiles(grid_generator):
    # Define the extent of the polygon
    extent = [-3.715, 40.410, -3.694, 40.425]

    # Generate the grid cells covering the extent
    result = grid_generator.cover_extent_with_grid_tiles(extent)

    # Assert that the result is a DataFrame
    assert isinstance(result, DataFrame)

    # Assert that the result DataFrame has the expected columns
    expected_columns = ["geometry", "grid_id"]
    assert result.columns == expected_columns

    # Assert that the result DataFrame
    assert result.count() == 713


@pytest.mark.skip(reason="TODO: New Grid implementation")
def test_grid_ids_to_centroids(grid_generator, centroid_grid):
    test_grid = centroid_grid.drop("geometry")

    test_grid = grid_generator.convert_inspire_specs_to_internal_id(test_grid)
    grid_ids_to_centroids = grid_generator.grid_ids_to_centroids(test_grid)
    grid_ids_to_centroids = grid_generator.convert_internal_id_to_inspire_specs(grid_ids_to_centroids)
    grid_ids_to_centroids = grid_ids_to_centroids.select("grid_id", "geometry")
    assert_sparkgeodataframe_equal(centroid_grid, grid_ids_to_centroids)


# TODO: Review
# def test_grid_ids_to_tiles(grid_generator, tile_grid):
#     test_grid = tile_grid.drop("geometry")

#     grid_ids_to_tiles = grid_generator.grid_ids_to_tiles(test_grid)

#     with pytest.raises(AssertionError):
#         assert_sparkgeodataframe_equal(tile_grid, grid_ids_to_tiles)

#     assert_geodataframe_almost_equal(tile_grid, grid_ids_to_tiles)


def test_grid_id_to_inspire_id(spark):
    # Create test DataFrame
    data = [
        Row(grid_id=100, origin=0),
        Row(grid_id=200, origin=0),
        Row(grid_id=300, origin=0),
    ]
    schema = StructType(
        [
            StructField("grid_id", IntegerType(), True),
            StructField("origin", IntegerType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)

    # Create InspireGridGenerator instance
    grid_generator = InspireGridGenerator(spark)

    # Call the grid_id_to_inspire_id function
    result = grid_generator.grid_id_to_inspire_id(df, inspire_resolution=100)

    # Define the expected result DataFrame
    expected_data = [
        Row(grid_id=100, origin=0, INSPIRE_id="100mN0E100"),
        Row(grid_id=200, origin=0, INSPIRE_id="100mN0E200"),
        Row(grid_id=300, origin=0, INSPIRE_id="100mN0E300"),
    ]
    schema = StructType(
        [
            StructField("grid_id", IntegerType(), True),
            StructField("origin", IntegerType(), True),
            StructField("INSPIRE_id", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_data, schema=schema)

    # Assert that the result DataFrame is equal to the expected DataFrame
    assertDataFrameEqual(result, expected_df)
