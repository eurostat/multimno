"""

"""

from typing import List
from sedona.sql import st_functions as STF
from sedona.sql.types import GeometryType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeMccIsoTzMap(ParquetDataObject):
    """
    Class that models country polygons spatial data.
    """

    ID = "BronzeMccIsoTzMapDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.mcc, IntegerType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=False),
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.iso3, StringType(), nullable=True),
            StructField(ColNames.eurostat_code, StringType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.timezone, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = []
