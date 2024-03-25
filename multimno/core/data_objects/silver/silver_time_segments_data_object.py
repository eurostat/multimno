"""

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
    ArrayType,
    BooleanType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverTimeSegmentsDataObject(PathDataObject):
    """
    Class that models the cleaned MNO Event data.
    """

    ID = "SilverTimeSegmentsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.time_segment_id, IntegerType(), nullable=False),
            StructField(ColNames.start_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.end_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.mcc, ShortType(), nullable=False),
            StructField(ColNames.cells, ArrayType(StringType()), nullable=False),
            StructField(ColNames.state, StringType(), nullable=False),
            StructField(ColNames.is_last, BooleanType(), nullable=True),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: list[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = partition_columns

        # Clear path
        self.first_write = True

    def write(self, path: str = None, partition_columns: list[str] = None):
        # If it is the first writing of this data object, clear the input directory, otherwise add
        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
