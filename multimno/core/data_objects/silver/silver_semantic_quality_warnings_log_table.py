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
    BooleanType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityWarningsLogTable(ParquetDataObject):
    """
    Class that models the log table keeping track of the quality warnings that may arise from
    the semantic checks of the MNO Event data.
    """

    ID = "SilverEventSemanticQualityWarningsLogTable"

    SCHEMA = StructType(
        [
            StructField("date", DateType(), nullable=False),
            StructField("Error 1", FloatType(), nullable=False),
            StructField("Error 2", FloatType(), nullable=False),
            StructField("Error 3", FloatType(), nullable=False),
            StructField("Error 4", FloatType(), nullable=False),
            StructField("Error 5", FloatType(), nullable=False),
            StructField("Error 1 upper control limit", FloatType(), nullable=True),
            StructField("Error 2 upper control limit", FloatType(), nullable=True),
            StructField("Error 3 upper control limit", FloatType(), nullable=True),
            StructField("Error 4 upper control limit", FloatType(), nullable=True),
            StructField("Error 5 upper control limit", FloatType(), nullable=True),
            StructField("Error 1 display warning", BooleanType(), nullable=False),
            StructField("Error 2 display warning", BooleanType(), nullable=False),
            StructField("Error 3 display warning", BooleanType(), nullable=False),
            StructField("Error 4 display warning", BooleanType(), nullable=False),
            StructField("Error 5 display warning", BooleanType(), nullable=False),
            StructField("execution_id", TimestampType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
