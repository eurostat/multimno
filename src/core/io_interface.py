from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from sedona.spark import ShapefileReader, Adapter


class IOInterface(metaclass=ABCMeta):
    """Abstract interface that provides functionality for reading and writing data


    """
    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        if cls is IOInterface:
            attrs: list[str] = []
            callables: list[str] = [
                'read_from_interface', 'write_from_interface']
            ret: bool = True
            for attr in attrs:
                ret = ret and (hasattr(subclass, attr)
                               and isinstance(getattr(subclass, attr), property))
            for call in callables:
                ret = ret and (hasattr(subclass, call)
                               and callable(getattr(subclass, call)))
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
    FILE_FORMAT = ''

    def read_from_interface(self, spark: SparkSession, path: str, schema: StructType = None):
        # changed to make possible to read input that do not have schema (this is the case of landing DO)
        return spark.read.load(
            path,
            self.FILE_FORMAT, 
            schema
        )

    def write_from_interface(self, df: DataFrame, path: str, partition_columns: list[str] = None):
        # Args check
        if partition_columns is None:
            partition_columns = []

        df.write.format(
            self.FILE_FORMAT,  # File format
        ).partitionBy(
            partition_columns
        ).mode("overwrite").save(path)


class ParquetInterface(PathInterface):
    FILE_FORMAT = 'parquet'


class JsonInterface(PathInterface):
    FILE_FORMAT = 'json'


class ShapefileInterface(PathInterface):
    def read_from_interface(self, spark: SparkSession, path: str, schema: StructType = None):
        df = ShapefileReader.readToGeometryRDD(spark.sparkContext, path)
        return Adapter.toDf(df, spark)

    def write_from_interface(self, df: DataFrame, path: str, partition_columns: list = None):
        raise NotImplementedError(
            "Not implemented as Shapefiles shouldn't be written")


class CsvInterface(PathInterface):
    def read_from_interface(self, path: str, schema: StructType, header: bool, sep: str = ','):
        return self.spark.read.csv(path,
                                   schema=schema,
                                   header=header,
                                   sep=sep)


class GeoParquetInterface(PathInterface):
    FILE_FORMAT = 'geoparquet'
