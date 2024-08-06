"""
Silver Network Data Top Frequent Errors.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    StringType,
    FloatType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverNetworkDataTopFrequentErrors(PathDataObject):
    """
    Class that models the Silver Network Topology Top Frequent Errors data object
    """

    ID = "SilverNetworkDataTopFrequentErrorsDO"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.field_name, StringType(), nullable=False),
            StructField(ColNames.type_code, IntegerType(), nullable=False),
            StructField(ColNames.error_value, StringType(), nullable=False),
            StructField(ColNames.error_count, IntegerType(), nullable=False),
            StructField(ColNames.accumulated_percentage, FloatType(), nullable=False),
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
