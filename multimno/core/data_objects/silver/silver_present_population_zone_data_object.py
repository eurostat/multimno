from typing import List

"""
Silver present population estimatation per zone data object
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


class SilverPresentPopulationZoneDataObject(PathDataObject):
    """
    Estimation of the population present at a given time at the level of some zoning system.
    """

    ID = "SilverPresentPopulationZoneDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.population, FloatType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    VALUE_COLUMNS = [ColNames.population]
    
    AGGREGATION_COLUMNS = [ColNames.zone_id,
                            ColNames.timestamp,
                            ColNames.dataset_id,
                            ColNames.level,
                            ColNames.year,
                            ColNames.month,
                            ColNames.day]
    
    PARTITION_COLUMNS = [ColNames.dataset_id, 
                         ColNames.level, 
                         ColNames.year, 
                         ColNames.month, 
                         ColNames.day]

    def __init__(
        self, spark: SparkSession, default_path: str, partition_columns: List[str] = None, mode: str = "overwrite"
    ) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        if partition_columns is None:
            partition_columns = self.PARTITION_COLUMNS
        self.partition_columns = partition_columns
        self.mode = mode

    def write(self, path: str = None, partition_columns: list[str] = None, mode: str = None) -> None:
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns
        if mode is None:
            mode = self.mode

        self.interface.write_from_interface(self.df, path, partition_columns)
