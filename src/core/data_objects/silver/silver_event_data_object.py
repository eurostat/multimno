from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
)

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface


class SilverEventDataObject(PathDataObject):
    ID = "SilverEventDO"
    SCHEMA = StructType(
        [
            StructField("user_id", BinaryType(), nullable=False),
            StructField("timestamp", TimestampType(), nullable=False),
            StructField("mcc", IntegerType(), nullable=False),
            StructField("cell_id", StringType(), nullable=True),
            StructField("latitude", FloatType(), nullable=True),
            StructField("longitude", FloatType(), nullable=True),
            StructField("loc_error", FloatType(), nullable=True),
            StructField("year", ShortType(), nullable=False),
            StructField("month", ByteType(), nullable=False),
            StructField("day", ByteType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = ["year", "month", "day"]

        # Clear path
        self.first_write = True

    def write(self, path: str = None, partition_columns: list[str] = None):
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
