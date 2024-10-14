"""
Bronze MNO Event data module
"""

from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BinaryType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class BronzeEventDataObject(ParquetDataObject):
    """
    Class that models the RAW MNO Event data.
    """

    ID = "BronzeEventDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=True),
            StructField(ColNames.timestamp, StringType(), nullable=True),
            StructField(ColNames.mcc, IntegerType(), nullable=True),
            StructField(ColNames.mnc, StringType(), nullable=True),
            StructField(ColNames.plmn, IntegerType(), nullable=True),
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.loc_error, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
