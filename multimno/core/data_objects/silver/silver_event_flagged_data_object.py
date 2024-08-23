"""
Silver MNO Event data module with flags computed in Semantic Checks
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverEventFlaggedDataObject(PathDataObject):
    """
    Class that models the cleaned MNO Event data, with flags computed
    in the semantic checks module.
    """

    ID = "SilverEventFlaggedDO"

    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
            StructField(ColNames.mcc, IntegerType(), nullable=True),
            StructField(ColNames.mnc, StringType(), nullable=True),
            StructField(ColNames.plmn, IntegerType(), nullable=True),
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.loc_error, FloatType(), nullable=True),
            StructField(ColNames.error_flag, IntegerType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, mode: str = 'overwrite') -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = [
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id_modulo,
        ]
        self.mode = mode

    def write(self, path: str = None, partition_columns: list[str] = None, mode: str = None) -> None:
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns
        if mode is None:
            mode = self.mode

        self.interface.write_from_interface(self.df, path, partition_columns, mode)
