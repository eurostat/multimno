from typing import List
"""

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ShortType, ByteType, DateType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverSignalStrengthDataObject(PathDataObject):
    """ """

    ID = "SilverSignalStrengthDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, StringType(), nullable=True),
            StructField(ColNames.valid_date_start, DateType(), nullable=True),
            StructField(ColNames.valid_date_end, DateType(), nullable=True),
            StructField(ColNames.signal_strength, FloatType(), nullable=True),
            StructField(ColNames.distance_to_cell, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.cell_id,
        ColNames.grid_id,
        ColNames.valid_date_start,
        ColNames.valid_date_end,
        ColNames.signal_strength,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    OPTIONAL_COLUMNS = [ColNames.distance_to_cell]

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: List[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = partition_columns

    def write(self, path: str = None, partition_columns: List[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
