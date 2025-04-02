"""
Module that defines the data object abstract classes
"""

from abc import ABCMeta
from typing import List

from multimno.core.constants.spatial import INSPIRE_GRID_EPSG
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from sedona.sql import st_functions as STF

from multimno.core.constants.columns import ColNames
from multimno.core.spark_session import SPARK_WRITING_MODES
from multimno.core.io_interface import GeoParquetInterface, IOInterface, ParquetInterface, PathInterface


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
        return self

    def write(self, *args, **kwargs):
        """
        Method that performs the write operation of the data object dataframe through an IOInterface.
        """
        self.interface.write_from_interface(self.df, *args, **kwargs)

    def cast_to_schema(self):
        columns = {field.name: F.col(field.name).cast(field.dataType) for field in self.SCHEMA.fields}
        self.df = self.df.withColumns(columns)


class PathDataObject(DataObject, metaclass=ABCMeta):
    """Abstract Class that models DataObjects that will use a PathInterface for IO operations.
    It inherits the DataObject abstract class.
    """

    ID = ...
    SCHEMA = ...
    PARTITION_COLUMNS = ...

    def __init__(
        self,
        spark: SparkSession,
        default_path: str,
        default_partition_columns: List[str] = None,
        default_mode: str = SPARK_WRITING_MODES.APPEND,
    ) -> None:
        super().__init__(spark)
        self.interface: PathInterface = None
        self.default_path: str = default_path
        if default_partition_columns is None:
            default_partition_columns = self.PARTITION_COLUMNS
        self.default_partition_columns: List[str] = default_partition_columns
        self.default_mode: str = default_mode

    def read(self, *args, path: str = None, **kwargs):
        if path is None:
            path = self.default_path
        self.df = self.interface.read_from_interface(self.spark, path, self.SCHEMA)

        return self

    def write(self, *args, path: str = None, partition_columns: list[str] = None, mode: str = None, **kwargs):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.default_partition_columns
        if mode is None:
            mode = self.default_mode

        self.interface.write_from_interface(self.df, path=path, partition_columns=partition_columns, mode=mode)

    def get_size(self) -> int:
        """
        Returns the size of the data object in bytes.
        """
        files = self.df.inputFiles()

        if len(files) == 0:
            return 0

        conf = self.spark._jsc.hadoopConfiguration()
        # need to get proper URI prefix for the file system
        uri = self.spark._jvm.java.net.URI.create(files[0])
        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        total_size = 0

        for file in files:
            total_size += fs.getFileStatus(self.spark._jvm.org.apache.hadoop.fs.Path(file)).getLen()

        return total_size

    def get_num_files(self) -> int:
        """
        Returns the number of files of the data object.
        """
        return len(self.df.inputFiles())

    def get_top_rows(self, n: int, truncate: int = 20) -> str:
        """
        Returns string with top n rows. Same as df.show.
        """
        return self.df._jdf.showString(n, truncate, False)


class ParquetDataObject(PathDataObject):
    """
    Class that models a DataObject that will use a ParquetInterface for IO operations.
    It inherits the PathDataObject abstract class.
    """

    def __init__(
        self,
        spark: SparkSession,
        default_path: str,
        default_partition_columns: List[str] = None,
        default_mode: str = SPARK_WRITING_MODES.APPEND,
    ) -> None:
        super().__init__(spark, default_path, default_partition_columns, default_mode)
        self.interface: PathInterface = ParquetInterface()


class GeoParquetDataObject(PathDataObject):
    """
    Class that models a DataObject that will use a ParquetInterface for IO operations.
    It inherits the PathDataObject abstract class.
    """

    def __init__(
        self,
        spark: SparkSession,
        default_path: str,
        default_partition_columns: List[str] = None,
        default_mode: str = SPARK_WRITING_MODES.APPEND,
        default_crs: int = INSPIRE_GRID_EPSG,
        set_crs: bool = True,
    ) -> None:
        super().__init__(spark, default_path, default_partition_columns, default_mode)
        self.interface: PathInterface = GeoParquetInterface()
        self.default_crs = default_crs
        self.set_crs = set_crs

    def read(self):
        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.SCHEMA)

        if self.set_crs:
            self.df = self.df.withColumn(
                ColNames.geometry, STF.ST_SetSRID((ColNames.geometry), F.lit(self.default_crs))
            )

        return self
