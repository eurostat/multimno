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

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverDeviceActivityStatistics(PathDataObject):
    """
    Class that models the cleaned MNO Event data.
    """

    ID = "SilverDeviceActivityStatisticsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.event_cnt, IntegerType(), nullable=False),
            StructField(ColNames.unique_cell_cnt, ShortType(), nullable=False),
            StructField(ColNames.unique_location_cnt, ShortType(), nullable=False),
            StructField(ColNames.sum_distance_m, IntegerType(), nullable=True),
            StructField(ColNames.unique_hour_cnt, ByteType(), nullable=False),
            StructField(ColNames.mean_time_gap, IntegerType(), nullable=True),
            StructField(ColNames.stdev_time_gap, FloatType(), nullable=True),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [ColNames.year, ColNames.month, ColNames.day]

    def write(self, path: str = None, partition_columns: List[str] = None):
        # If it is the first writing of this data object, clear the input directory, otherwise add
        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.df.write.format(
            self.interface.FILE_FORMAT,  # File format
        ).partitionBy(self.partition_columns).mode(
            "append"
        ).save(path)
