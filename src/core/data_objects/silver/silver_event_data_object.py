from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BinaryType, IntegerType

from core.data_objects.data_object import DataObject
from core.io_interface import ParquetInterface


class SilverEventDataObject(DataObject):
    ID = "SilverEventDO"
    SCHEMA = StructType([
        StructField("year", IntegerType(), nullable=False),
        StructField("month", IntegerType(), nullable=False),
        StructField("day", IntegerType(), nullable=False),
        StructField("user_id", BinaryType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
        StructField("mcc", IntegerType(), nullable=False),
        StructField("cell_id", StringType(), nullable=False),
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True),
        StructField("loc_error", FloatType(), nullable=True)
    ])

    def __init__(self, spark: SparkSession, path: str) -> None:
        super().__init__()
        self.interface: ParquetInterface = ParquetInterface(spark, path, self.SCHEMA)
        self.partition_columns = ["year", "month", "day"]

    def write(self):
        self.interface.write_from_interface(partition_columns=self.partition_columns)