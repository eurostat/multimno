"""
Bronze Synthetic Diaries Data module
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ShortType,
    ByteType,
    TimestampType,
    BinaryType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeSyntheticDiariesDataObject(ParquetDataObject):
    """
    Class that models synthetically-generated agents activity-trip diaries.

            ''''''

    """

    ID = "BronzeSyntheticDiariesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.activity_type, StringType(), nullable=True),
            StructField(ColNames.stay_type, StringType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.initial_timestamp, TimestampType(), nullable=True),
            StructField(ColNames.final_timestamp, TimestampType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
