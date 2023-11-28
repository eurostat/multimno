from abc import ABCMeta, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from sedona.spark import ShapefileReader, Adapter


class IOInterface(metaclass=ABCMeta):

    @abstractmethod
    def read_from_interface(self) -> DataFrame:
        pass

    @abstractmethod
    def write_from_interface(self, df: DataFrame):
        pass


class PathInterface(IOInterface, metaclass=ABCMeta):
    def __init__(self, spark: SparkSession,  path: str, schema: dict) -> None:
        super().__init__()
        self.path: str = path
        self.spark: SparkSession = spark
        self.schema = schema
        self.format: str = None

    def read_from_interface(self, path:str=None):
        if path is None:
            path = self.path
        return self.spark.read.schema(
            self.schema  # Read schema
        ).format(
            self.format  # File format
        ).load(
            path  # Load path
        )

    def write_from_interface(self, df: DataFrame, path:str=None):
        if path is None:
            path = self.path
        df.write.format(
            self.format,  # File format
        ).mode("overwrite").save(path)


class ParquetInterface(PathInterface):
    def __init__(self, spark: SparkSession,  path: str, schema: dict) -> None:
        super().__init__(spark, path, schema)
        self.format = 'parquet'

    def write_from_interface(self, df: DataFrame, path:str=None, partition_columns: list[str]= None):
        # Args check
        if path is None:
            path = self.path
        if partition_columns is None:
            partition_columns = []

        # Write
        df.write.format(
            self.format,  # File format
        ).partitionBy(
            partition_columns
        ).mode("overwrite").save(path)

class JsonInterface(PathInterface):
    def __init__(self, spark: SparkSession,  path: str, schema: dict) -> None:
        super().__init__(spark, path, schema)
        self.format = 'json'

class ShapefileInterface(PathInterface):
    def __init__(self, spark: SparkSession,  path: str, schema: dict) -> None:
        super().__init__(spark, path, schema)

    def read_from_interface(self):
        df = ShapefileReader.readToGeometryRDD(self.spark.sparkContext, self.path)
        return Adapter.toDf(df, self.spark)
    
    # TODO: Write to shapefile

class CsvInterface(PathInterface):
    def __init__(self, spark: SparkSession,  path: str, schema: dict, delimiter: str = ',', header: bool=True) -> None:
        super().__init__(spark, path, schema)
        self.format = 'csv'
        self.delimiter = delimiter
        self.header = header

    def read_from_interface(self):
        return self.spark.read.csv(self.path, 
                                   schema=self.schema, 
                                   header=self.header,
                                   sep=self.delimiter)
    
    # TODO: Write to csv


class GeoParquetInterface(PathInterface):
    def __init__(self, spark: SparkSession,  path: str, schema: dict) -> None:
        super().__init__(spark, path, schema)
        self.format = 'geoparquet'
