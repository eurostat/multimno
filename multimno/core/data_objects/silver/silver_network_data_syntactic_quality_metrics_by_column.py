"""
Silver Network topology quality metrics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    DateType,
    IntegerType,
    ShortType,
    ByteType,
)


from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverNetworkDataQualityMetricsByColumn(PathDataObject):
    """
    Class that models the Silver Network Topology data quality metrics data object.
    """

    ID = "SilverNetworkDataQualityMetricsByColumn"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.field_name, StringType(), nullable=True),
            StructField(ColNames.type_code, IntegerType(), nullable=False),
            StructField(ColNames.value, IntegerType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: list[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = partition_columns

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
