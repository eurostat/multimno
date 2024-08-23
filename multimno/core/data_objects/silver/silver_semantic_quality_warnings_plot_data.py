from typing import List
"""
Silver MNO Network Topology Quality Warnings Log Table Data Object
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    ByteType,
    ShortType,
    TimestampType,
    DateType,
    StringType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityWarningsBarPlotData(PathDataObject):
    """
    Class that models the data required to produce line plots reflecting the daily evolution of the number
    of each type of error, as well as records without errors, as seen during the semantic checks of MNO Event data.
    """

    ID = "SilverEventSemanticQualityWarningBarPlotData"
    SCHEMA = StructType(
        [
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.type_of_error, StringType(), nullable=False),
            StructField(ColNames.value, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.variable, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = [ColNames.variable, ColNames.year, ColNames.month, ColNames.day, ColNames.timestamp]

    def write(self, path: str = None, partition_columns: List[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
