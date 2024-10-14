"""
Silver MNO Network Topology Quality Warnings Log Table Data Object
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    ByteType,
    ShortType,
    TimestampType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverNetworkDataSyntacticQualityWarningsLogTable(ParquetDataObject):
    """
    Class that models the log table keeping track of the quality warnings that may arise from
    the syntactic checks and cleaning of the MNO Network Topology Data.
    """

    ID = "SilverNetworkDataSyntacticQualityWarningsLogTable"
    SCHEMA = StructType(
        [
            StructField(ColNames.title, StringType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),  # date of study analysed
            StructField(ColNames.timestamp, TimestampType(), nullable=False),  # moment when QW where generated
            StructField(ColNames.measure_definition, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.condition, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),  # using same name as for events
            StructField(ColNames.condition_value, FloatType(), nullable=False),
            StructField(ColNames.warning_text, StringType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
