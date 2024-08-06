"""
Silver Daily Permanence Score data module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
    StringType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverDailyPermanenceScoreDataObject(PathDataObject):
    """
    Class that models the Daily Permanence Score data.
    """

    ID = "SilverDailyPermanenceScoreDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.time_slot_initial_time, TimestampType(), nullable=False),
            StructField(ColNames.time_slot_end_time, TimestampType(), nullable=False),
            StructField(ColNames.dps, ByteType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id_modulo,
            ColNames.id_type,
        ]

        # Clear path
        self.first_write = True

    def write(self, path: str = None, partition_columns: list[str] = None):
        # If it is the first writing of this data object, clear the input directory, otherwise add
        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
