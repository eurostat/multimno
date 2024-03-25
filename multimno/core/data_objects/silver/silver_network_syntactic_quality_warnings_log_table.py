"""
Silver MNO Network Topology Quality Warnings Log Table Data Object
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    IntegerType,
    StringType,
    ByteType,
    ShortType,
    TimestampType,
    DateType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverNetworkDataSyntacticQualityWarningsLogTable(PathDataObject):
    """
    Class that models the log table keeping track of the quality warnings that may arise from
    the syntactic checks and cleaning of the MNO Network Topology Data.
    """

    ID = "SilverNetworkDataSyntacticQualityWarningsLogTable"
    SCHEMA = StructType(
        [
            StructField(ColNames.title, StringType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),  # date of study analysed
            StructField(ColNames.timestamp, TimestampType(), nullable=False),  # moment when QW where generated
            StructField(ColNames.measure_definition, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.condition, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),  # using same name as for events
            StructField(ColNames.condition_value, FloatType(), nullable=False),
            StructField(ColNames.warning_text, StringType(), nullable=False),
            # partition columns
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
            "append"
        ).save(path)
