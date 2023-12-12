from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BinaryType, IntegerType, DateType

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface


class SilverEventDataSyntacticQualityMetricsFrequencyDistribution(PathDataObject):
    ID = "SilverEventDataSyntacticQualityMetricsFrequencyDistribution"
    SCHEMA = StructType([
        StructField("cell_id", StringType(), nullable=True),
        StructField("user_id", BinaryType(), nullable=True),
        StructField("initial_frequency", IntegerType(), nullable=False),
        StructField("final_frequency", IntegerType(), nullable=False), 
        StructField("date", DateType(), nullable=False)
    ])

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = None

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
