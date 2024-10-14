"""
Silver MNO Network Topology Quality Warnings Data Object for the generation of plots
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    ByteType,
    ShortType,
    IntegerType,
    TimestampType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


# TODO: Review these warnings data objects
class SilverNetworkSyntacticQualityWarningsLinePlotData(ParquetDataObject):
    """
    Class that models the data required to produce line plots reflecting the daily evolution of the number
    of rows before and after the syntactic checks, as well as the overall error rate.
    """

    ID = "SilverNetworkSyntacticQualityWarningsLinePlotData"
    SCHEMA = StructType(
        [
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.average, FloatType(), nullable=False),
            StructField(ColNames.LCL, FloatType(), nullable=False),
            StructField(ColNames.UCL, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.variable,
        ColNames.year,
        ColNames.month,
        ColNames.day,
        ColNames.timestamp,
    ]


class SilverNetworkSyntacticQualityWarningsPiePlotData(ParquetDataObject):
    """
    Class that models the data required to produce pie plots reflecting the percentage of each type of error
    for each field of the network topology data object.
    """

    ID = "SilverNetworkSyntacticQualityWarningsPiePlotData"
    SCHEMA = StructType(
        [
            StructField(ColNames.type_of_error, StringType(), nullable=False),
            StructField(ColNames.value, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.variable,
        ColNames.year,
        ColNames.month,
        ColNames.day,
        ColNames.timestamp,
    ]
