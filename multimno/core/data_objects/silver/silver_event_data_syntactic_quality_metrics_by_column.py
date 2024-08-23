from typing import List

"""
Silver Event Data quality metrics.
"""

from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    ShortType,
    DateType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventDataSyntacticQualityMetricsByColumn(PathDataObject):
    """
    Class that models the Silver Event Data quality metrics DataObject.
    """

    ID = "SilverEventDataSyntacticQualityMetricsByColumn"
    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.variable, StringType(), nullable=True),
            StructField(ColNames.type_of_error, ShortType(), nullable=True),
            StructField(ColNames.value, IntegerType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, mode="overwrite") -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = [ColNames.date]
        self.mode = mode

    def write(self, path: str = None, partition_columns: List[str] = None, mode=None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns
        if mode is None:
            mode = self.mode

        self.interface.write_from_interface(self.df, path, partition_columns, mode)
