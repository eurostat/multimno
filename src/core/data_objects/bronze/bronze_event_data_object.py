from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BinaryType

from core.data_objects.data_object import DataObject
from core.io_interface import ParquetInterface


class BronzeEventDataObject(DataObject):
    ID = "BronzeEventDO"
    SCHEMA = StructType([
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

    def write(self, path: str = None):
        self.interface.write_from_interface(self.df, path)