"""
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    ShortType,
    ByteType,
    StringType,
    TimestampType,
    LongType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityMetrics(ParquetDataObject):
    """ """

    ID = "SilverEventSemanticQualityMetrics"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.type_of_error, IntegerType(), nullable=False),
            StructField(ColNames.value, LongType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
