"""
Module that implements classes for reading data from different data sources into a Spark DataFrames.
"""

from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from sedona.spark import ShapefileReader, Adapter


class IOInterface(metaclass=ABCMeta):
    """Abstract interface that provides functionality for reading and writing data"""

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        if cls is IOInterface:
            attrs: list[str] = []
            callables: list[str] = ["read_from_interface", "write_from_interface"]
            ret: bool = True
            for attr in attrs:
                ret = ret and (hasattr(subclass, attr) and isinstance(getattr(subclass, attr), property))
            for call in callables:
                ret = ret and (hasattr(subclass, call) and callable(getattr(subclass, call)))
            return ret
        else:
            return NotImplemented

    @abstractmethod
    def read_from_interface(self, *args, **kwargs) -> DataFrame:
        pass

    @abstractmethod
    def write_from_interface(self, df: DataFrame, *args, **kwargs):
        pass


class PathInterface(IOInterface, metaclass=ABCMeta):
    """Abstract interface for reading/writing data from a file type data source."""

    FILE_FORMAT = ""

    def read_from_interface(self, spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
        """Method that reads data from a file type data source as a Spark DataFrame.

        Args:
            spark (SparkSession): Spark session.
            path (str): Path to the data.
            schema (StructType, optional): Schema of the data. Defaults to None.

        Returns:
            df: Spark dataframe.
        """
        return spark.read.schema(schema).format(self.FILE_FORMAT).load(path)  # Read schema  # File format  # Load path

    def write_from_interface(self, df: DataFrame, path: str, partition_columns: list[str] = None):
        """Method that writes data from a Spark DataFrame to a file type data source.

        Args:
            df (DataFrame): Spark DataFrame.
            path (str): Path to the data.
            partition_columns (list[str], optional): columns used for a partition write.
        """
        # Args check
        if partition_columns is None:
            partition_columns = []

        df.write.format(
            self.FILE_FORMAT,  # File format
        ).partitionBy(partition_columns).mode(
            "overwrite"
        ).save(path)


class ParquetInterface(PathInterface):
    """Class that implements the PathInterface abstract class for reading/writing data from a parquet data source."""

    FILE_FORMAT = "parquet"


class JsonInterface(PathInterface):
    """Class that implements the PathInterface abstract class for reading/writing data from a json data source."""

    FILE_FORMAT = "json"


class ShapefileInterface(PathInterface):
    """Class that implements the PathInterface abstract class for reading/writing data from a ShapeFile data source."""

    def read_from_interface(self, spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
        """Method that reads data from a ShapeFile type data source as a Spark DataFrame.

        Args:
            spark (SparkSession): Spark session.
            path (str): Path to the data.
            schema (StructType, optional): Schema of the data. Defaults to None.

        Returns:
            df: Spark dataframe.
        """
        df = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
        return Adapter.toDf(df, spark)

    def write_from_interface(self, df: DataFrame, path: str, partition_columns: list = None):
        """Method that writes data from a Spark DataFrame to a ShapeFile data source.

        Args:
            df (DataFrame): Spark DataFrame.
            path (str): Path to the data.
            partition_columns (list[str], optional): columns used for a partition write.
        Raises:
            NotImplementedError: ShapeFile files should not be written in this architecture.
        """
        raise NotImplementedError("Not implemented as Shapefiles shouldn't be written")


class CsvInterface(PathInterface):
    """Class that implements the PathInterface abstract class for reading/writing data from a csv data source."""

    FILE_FORMAT = "csv"

    def read_from_interface(
        self, spark: SparkSession, path: str, schema: StructType, header: bool = True, sep: str = ","
    ) -> DataFrame:
        """Method that reads data from a csv type data source as a Spark DataFrame.

        Args:
            spark (SparkSession): Spark session.
            path (str): Path to the data.
            schema (StructType, optional): Schema of the data. Defaults to None.

        Returns:
            df: Spark dataframe.
        """
        return spark.read.csv(path, schema=schema, header=header, sep=sep)

    def write_from_interface(
        self, df: DataFrame, path: str, partition_columns: list[str] = None, header: bool = True, sep: str = ","
    ):
        """Method that writes data from a Spark DataFrame to a csv data source.

        Args:
            df (DataFrame): Spark DataFrame.
            path (str): Path to the data.
            partition_columns (list[str], optional): columns used for a partition write.
        Raises:
            NotImplementedError: csv files should not be written in this architecture.
        """
        if partition_columns is None:
            partition_columns = []
        df.write.option("header", header).option("sep", sep).mode("overwrite").format("csv").save(path)


class GeoParquetInterface(PathInterface):
    """Class that implements the PathInterface abstract class for reading/writing data from a geoparquet data source."""

    FILE_FORMAT = "geoparquet"
