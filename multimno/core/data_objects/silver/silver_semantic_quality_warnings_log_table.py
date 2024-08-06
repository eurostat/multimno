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
    BooleanType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventSemanticQualityWarningsLogTable(PathDataObject):
    """
    Class that models the log table keeping track of the quality warnings that may arise from
    the semantic checks of the MNO Event data.
    """

    ID = "SilverEventSemanticQualityWarningsLogTable"

    SCHEMA = StructType(
        [
            StructField("date", DateType(), nullable=False),
            StructField("Error 1", FloatType(), nullable=False),
            StructField("Error 2", FloatType(), nullable=False),
            StructField("Error 3", FloatType(), nullable=False),
            StructField("Error 4", FloatType(), nullable=False),
            StructField("Error 5", FloatType(), nullable=False),
            StructField("Error 1 upper control limit", FloatType(), nullable=True),
            StructField("Error 2 upper control limit", FloatType(), nullable=True),
            StructField("Error 3 upper control limit", FloatType(), nullable=True),
            StructField("Error 4 upper control limit", FloatType(), nullable=True),
            StructField("Error 5 upper control limit", FloatType(), nullable=True),
            StructField("Error 1 display warning", BooleanType(), nullable=False),
            StructField("Error 2 display warning", BooleanType(), nullable=False),
            StructField("Error 3 display warning", BooleanType(), nullable=False),
            StructField("Error 4 display warning", BooleanType(), nullable=False),
            StructField("Error 5 display warning", BooleanType(), nullable=False),
            StructField("execution_id", TimestampType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        default_path: str,
        partition_columns: list[str] = None,
    ) -> None:
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
