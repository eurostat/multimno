from abc import ABCMeta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from core.io_interface import IOInterface, PathInterface


class DataObject(metaclass=ABCMeta):
    ID: str = None
    SCHEMA: StructType = None

    def __init__(self, spark: SparkSession) -> None:
        self.df: DataFrame = None
        self.spark: SparkSession = spark
        self.interface: IOInterface = None

    def read(self, *args, **kwargs):
        self.df = self.interface.read_from_interface(*args, **kwargs)

    def write(self, *args, **kwargs):
        self.interface.write_from_interface(self.df, *args, **kwargs)

class PathDataObject(DataObject, metaclass=ABCMeta):
    def __init__(self, spark: SparkSession) -> None:
        super().__init__(spark)
        self.interface : PathInterface = None
        
    def read(self, path: str):
        self.df = self.interface.read_from_interface(self.spark, path, self.SCHEMA)

    def write(self, path: str, partition_columns: list[str]=None):
        self.interface.write_from_interface(self.df, path, partition_columns)