"""
Silver Mid Term Permanence Score data object module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    BinaryType,
    IntegerType,
    StringType,
    ShortType,
    ByteType,
    FloatType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverMidtermPermanenceScoreDataObject(PathDataObject):
    """
    Class that models the Midterm Permanence Score data object.
    """

    ID = "SilverMidtermPermanenceScoreDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.mps, IntegerType(), nullable=False),
            StructField(ColNames.frequency, IntegerType(), nullable=False),
            StructField(ColNames.regularity_mean, FloatType(), nullable=True),
            StructField(ColNames.regularity_std, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day_type, StringType(), nullable=False),
            StructField(ColNames.time_interval, StringType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [
            ColNames.year,
            ColNames.month,
            ColNames.day_type,
            ColNames.time_interval,
            ColNames.id_type,
            ColNames.user_id_modulo,
        ]

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
