from abc import ABCMeta

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from core.io_interface import IOInterface


class DataObject(metaclass=ABCMeta):
    ID: str = None
    SCHEMA: StructType = None

    def __init__(self) -> None:
        self.df: DataFrame = None
        self.interface: IOInterface = None

    def read(self, *args, **kwargs):
        self.df = self.interface.read_from_interface(*args, **kwargs)

    def write(self, *args, **kwargs):
        self.interface.write_from_interface(self.df, *args, **kwargs)
