"""
Silver Event Data quality warning for plots table.
"""

from collections import defaultdict
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


class SilverEventDataSyntacticQualityWarningsForPlots(PathDataObject):
    """
    Class that store data to plot raw, clean data sizez and error rate.
    """

    ID = "SilverEventDataSyntacticQualityWarningsForPlots"
    SCHEMA = StructType(
        [
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.type_of_qw, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.average, FloatType(), nullable=True),
            StructField(ColNames.LCL, FloatType(), nullable=True),
            StructField(ColNames.UCL, FloatType(), nullable=True),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [ColNames.date]

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.df.write.format(
            self.interface.FILE_FORMAT,
        ).partitionBy(partition_columns).mode(
            "append"
        ).save(path)
