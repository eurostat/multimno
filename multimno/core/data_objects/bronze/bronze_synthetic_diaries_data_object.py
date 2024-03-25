"""
Bronze Synthetic Diaries Data module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ShortType,
    ByteType,
    TimestampType,
    BinaryType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class BronzeSyntheticDiariesDataObject(PathDataObject):
    """
    Class that models synthetically-generated agents activity-trip diaries.

            ''''''

    """

    ID = "BronzeSyntheticDiariesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.activity_type, StringType(), nullable=True),
            StructField(ColNames.stay_type, StringType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.initial_timestamp, TimestampType(), nullable=True),
            StructField(ColNames.final_timestamp, TimestampType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: list[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = partition_columns

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
