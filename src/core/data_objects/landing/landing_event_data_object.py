from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface


class LandingEventDataObject(PathDataObject):
    ID = "LandingDO"
    SCHEMA: StructType = None

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
