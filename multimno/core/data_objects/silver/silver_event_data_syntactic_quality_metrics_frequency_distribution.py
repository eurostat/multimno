from typing import List
"""
Silver Event Data deduplication frequency quality metrics.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BinaryType,
    IntegerType,
    DateType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface


class SilverEventDataSyntacticQualityMetricsFrequencyDistribution(PathDataObject):
    """
    Class that models the Silver Event Data syntactic
    frequency quality metrics DataObject.
    """

    ID = "SilverEventDataSyntacticQualityMetricsFrequencyDistribution"
    SCHEMA = StructType(
        [
            StructField("cell_id", StringType(), nullable=True),
            StructField("user_id", BinaryType(), nullable=True),
            StructField("initial_frequency", IntegerType(), nullable=False),
            StructField("final_frequency", IntegerType(), nullable=False),
            StructField("date", DateType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, mode="overwrite") -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = ["date"]
        self.mode = mode

    def write(self, path: str = None, partition_columns: List[str] = None, mode=None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns
        if mode is None:
            mode = self.mode

        self.interface.write_from_interface(self.df, path, partition_columns, mode)
