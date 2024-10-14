"""
Silver Network topology quality metrics.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    DateType,
    IntegerType,
    ShortType,
    ByteType,
)


from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverNetworkDataQualityMetricsByColumn(ParquetDataObject):
    """
    Class that models the Silver Network Topology data quality metrics data object.
    """

    ID = "SilverNetworkDataQualityMetricsByColumn"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.field_name, StringType(), nullable=True),
            StructField(ColNames.type_code, IntegerType(), nullable=False),
            StructField(ColNames.value, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
