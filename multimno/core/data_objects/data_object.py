"""
Module that defines the data object abstract classes
"""

from abc import ABCMeta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from multimno.core.io_interface import IOInterface, PathInterface


class DataObject(metaclass=ABCMeta):
    """
    Abstract class that models a DataObject. It defines its data schema including the attributes that compose it.
    """

    ID: str = None
    SCHEMA: StructType = None

    def __init__(self, spark: SparkSession) -> None:
        self.df: DataFrame = None
        self.spark: SparkSession = spark
        self.interface: IOInterface = None

    def read(self, *args, **kwargs):
        """
        Method that performs the read operation of the data object dataframe through an IOInterface.
        """
        self.df = self.interface.read_from_interface(*args, **kwargs)

    def write(self, *args, **kwargs):
        """
        Method that performs the write operation of the data object dataframe through an IOInterface.
        """
        self.interface.write_from_interface(self.df, *args, **kwargs)


class PathDataObject(DataObject, metaclass=ABCMeta):
    """Abstract Class that models DataObjects that will use a PathInterface for IO operations.
    It inherits the DataObject abstract class.
    """

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark)
        self.interface: PathInterface = None
        self.default_path: str = default_path

    def read(self, *args, path: str = None, **kwargs):
        if path is None:
            path = self.default_path
        self.df = self.interface.read_from_interface(self.spark, path, self.SCHEMA)

    def write(self, *args, path: str = None, partition_columns: list[str] = None, **kwargs):
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
