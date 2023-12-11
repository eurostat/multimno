from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BinaryType

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface


class BronzeEventDataObject(PathDataObject):
    ID = "BronzeEventDO"
    SCHEMA = StructType([
        StructField("user_id", BinaryType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("mcc", IntegerType(), nullable=True),
        StructField("cell_id", StringType(), nullable=True),
        StructField("latitude", FloatType(), nullable=True),
        StructField("longitude", FloatType(), nullable=True),
        StructField("loc_error", FloatType(), nullable=True)
    ])

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()

    

