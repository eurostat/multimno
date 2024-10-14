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
)

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeCountriesDataObject(GeoParquetDataObject):
    """
    Class that models country polygons spatial data.
    """

    ID = "BronzeCountriesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = []
