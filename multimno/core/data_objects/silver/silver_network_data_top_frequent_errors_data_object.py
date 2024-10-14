"""
Silver Network Data Top Frequent Errors.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    FloatType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverNetworkDataTopFrequentErrors(ParquetDataObject):
    """
    Class that models the Silver Network Topology Top Frequent Errors data object
    """

    ID = "SilverNetworkDataTopFrequentErrorsDO"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.field_name, StringType(), nullable=False),
            StructField(ColNames.type_code, IntegerType(), nullable=False),
            StructField(ColNames.error_value, StringType(), nullable=False),
            StructField(ColNames.error_count, IntegerType(), nullable=False),
            StructField(ColNames.accumulated_percentage, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
