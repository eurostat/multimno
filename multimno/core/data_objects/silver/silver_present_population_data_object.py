from typing import List
"""
Silver present population estimatation per grid data object
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    ByteType,
    ShortType,
    TimestampType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverPresentPopulationDataObject(PathDataObject):
    """
    Estimation of the population present at a given time at the grid tile level.
    """

    ID = "SilverPresentPopulationDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.population, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
        ]
    )

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
