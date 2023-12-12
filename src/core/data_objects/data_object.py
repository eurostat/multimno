from abc import ABCMeta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from core.io_interface import IOInterface, PathInterface


class DataObject(metaclass=ABCMeta):
    ID: str = None
    SCHEMA: StructType = None

    def __init__(self, spark: SparkSession, interface: IOInterface = None) -> None:
        self.df: DataFrame = None
        self.spark: SparkSession = spark
        self.interface = interface #= IoI

    def read(self, *args, **kwargs):
        self.df = self.interface.read_from_interface(*args, **kwargs)

    def write(self, *args, **kwargs):
        self.interface.write_from_interface(self.df, *args, **kwargs)


class PathDataObject(DataObject, metaclass=ABCMeta):
    def __init__(self, spark: SparkSession, default_path: str, interface: PathInterface = None) -> None:
        super().__init__(spark)
        self.interface = interface # PathInterface = None
        self.default_path: str = default_path

    def read(self, path: str = None):
        if path is None:
            path = self.default_path
        self.df = self.interface.read_from_interface(
            self.spark, path, self.SCHEMA)

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        
        self.interface.write_from_interface(df = current_df, path = path, partition_columns = partition_columns)
