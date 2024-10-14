from typing import List

"""
Silver Device Activity Statistics module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    FloatType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverDeviceActivityStatistics(ParquetDataObject):
    """
    Class that models the cleaned MNO Event data.
    """

    ID = "SilverDeviceActivityStatisticsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.event_cnt, IntegerType(), nullable=False),
            StructField(ColNames.unique_cell_cnt, ShortType(), nullable=False),
            StructField(ColNames.unique_location_cnt, ShortType(), nullable=False),
            StructField(ColNames.sum_distance_m, IntegerType(), nullable=True),
            StructField(ColNames.unique_hour_cnt, ByteType(), nullable=False),
            StructField(ColNames.mean_time_gap, IntegerType(), nullable=True),
            StructField(ColNames.stdev_time_gap, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
