""" """

from sedona.sql.types import GeometryType
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, MapType, IntegerType

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEnrichedGridDataObject(GeoParquetDataObject):
    """
    Class that models operational grid.
    """

    ID = "SilverEnrichedGridDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            StructField(ColNames.grid_id, IntegerType(), nullable=False),
            StructField(ColNames.elevation, FloatType(), nullable=True),
            StructField(ColNames.main_landuse_category, StringType(), nullable=True),
            StructField(ColNames.landuse_area_ratios, MapType(StringType(), FloatType()), nullable=True),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = [ColNames.quadkey]
