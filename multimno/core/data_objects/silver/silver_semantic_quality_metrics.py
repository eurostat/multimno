"""
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    ShortType,
    ByteType,
    StringType,
    TimestampType,
    LongType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityMetrics(PathDataObject):
    """ """

    ID = "SilverEventSemanticQualityMetrics"

    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.type_of_error, IntegerType(), nullable=False),
            StructField(ColNames.value, LongType(), nullable=False),
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

        # Always append
        self.df.write.format(
            self.interface.FILE_FORMAT,
        ).partitionBy(partition_columns).mode(
            "overwrite"
        ).save(path)
