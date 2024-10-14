"""
Silver Network Data Row Error Metrics.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverNetworkRowErrorMetrics(ParquetDataObject):
    """
    Class that models the Silver Network Topology Row Error Metrics data object
    """

    ID = "SilverNetworkRowErrorMetricsDO"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.value, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
