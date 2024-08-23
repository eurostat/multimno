"""

"""

from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import GeoParquetInterface
from multimno.core.constants.columns import ColNames


class LandingGeoParquetDataObject(PathDataObject):
    """
    Class that models input geospatial data.
    """

    ID = "LandingGeoParquetDO"

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: List[str] = None) -> None:

        super().__init__(spark, default_path)
        self.interface: GeoParquetInterface = GeoParquetInterface()
        self.partition_columns = partition_columns

    def read(self):

        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.SCHEMA)

    def write(self, path: str = None, partition_columns: List[str] = None):

        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
