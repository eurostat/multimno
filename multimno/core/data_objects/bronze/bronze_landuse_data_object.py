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
    DateType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeLanduseDataObject(GeoParquetDataObject):
    """
    Class that models landuse spatial data.
    """

    ID = "BronzeLanduseDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.category, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.quadkey]
