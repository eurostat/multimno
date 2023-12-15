from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BinaryType, IntegerType, ShortType, DateType

from collections import defaultdict

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface
from collections import defaultdict


class SilverEventDataSyntacticQualityMetricsByColumn(PathDataObject):
    ID = "SilverEventDataSyntacticQualityMetricsByColumn"
    SCHEMA = StructType([
        StructField("result_timestamp", TimestampType(), nullable=False),
        StructField("data_period_start", DateType(), nullable=False),
        StructField("data_period_end", DateType(), nullable=False),
        StructField("variable", StringType(), nullable=True),
        StructField("type_of_error", ShortType(), nullable=True),
        StructField("type_of_transformation", ShortType(), nullable=True),
        StructField("value", IntegerType(), nullable=False),
    ])

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = None

        # (variable, type_of_error, type_of_transformation) : value
        self.error_and_transformation_counts = defaultdict(int)

    def write(self, path: str = None, partition_columns: list[str] = None):

        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
