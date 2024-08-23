from typing import List
"""
Silver Event Data Quality Warning log table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventDataSyntacticQualityWarningsLogTable(PathDataObject):
    """
    Class that stores information about Event Quallity Warnings
    """

    ID = "SilverEventDataSyntacticQualityWarningsLogTable"
    SCHEMA = StructType(
        [
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.measure_definition, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.condition_value, FloatType(), nullable=True),
            StructField(ColNames.condition, StringType(), nullable=False),
            StructField(ColNames.warning_text, StringType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [ColNames.date]

    def write(self, path: str = None, partition_columns: List[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.df.write.format(
            self.interface.FILE_FORMAT,
        ).partitionBy(partition_columns).mode(
            "append"
        ).save(path)
