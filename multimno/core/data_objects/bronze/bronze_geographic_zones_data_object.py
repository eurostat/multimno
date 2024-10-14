"""

"""

from typing import List
from sedona.sql import st_functions as STF
from sedona.sql.types import GeometryType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeGeographicZonesDataObject(GeoParquetDataObject):
    """
    Class that models country polygons spatial data.
    """

    ID = "GeographicZonesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=True),
            StructField(ColNames.level, ShortType(), nullable=True),
            StructField(ColNames.parent_id, StringType(), nullable=True),
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.dataset_id, ColNames.year, ColNames.month, ColNames.day]
