from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BinaryType, IntegerType

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface


class SilverEventDataObject(PathDataObject):
    ID = "SilverEventDO"
    SCHEMA = StructType([
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("day", IntegerType(), nullable=False),
        StructField("user_id", BinaryType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False),
        StructField("mcc", IntegerType(), nullable=False),
        StructField("cell_id", StringType(), nullable=True),
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True),
        StructField("loc_error", FloatType(), nullable=True)
    ])

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = ["year", "month", "day"]

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
