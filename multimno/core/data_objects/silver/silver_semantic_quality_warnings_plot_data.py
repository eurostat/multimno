"""
Silver MNO Network Topology Quality Warnings Log Table Data Object
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    ByteType,
    ShortType,
    TimestampType,
    DateType,
    StringType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityWarningsBarPlotData(ParquetDataObject):
    """
    Class that models the data required to produce line plots reflecting the daily evolution of the number
    of each type of error, as well as records without errors, as seen during the semantic checks of MNO Event data.
    """

    ID = "SilverEventSemanticQualityWarningBarPlotData"
    SCHEMA = StructType(
        [
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.type_of_error, StringType(), nullable=False),
            StructField(ColNames.value, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.variable, ColNames.year, ColNames.month, ColNames.day, ColNames.timestamp]
