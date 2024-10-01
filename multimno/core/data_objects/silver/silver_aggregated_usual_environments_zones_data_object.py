from typing import List

"""
Silver usual environments estimatation per zone data object
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    DateType,
    ByteType
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverAggregatedUsualEnvironmentsZonesDataObject(PathDataObject):
    """
    Estimation of the population present at a given time at the level of some zoning system.
    """

    ID = "SilverAggregatedUsualEnvironmentsZonesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.weighted_device_count, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
        ]
    )

    VALUE_COLUMNS = [ColNames.weighted_device_count]

    AGGREGATION_COLUMNS = [ColNames.zone_id,
                            ColNames.dataset_id,
                            ColNames.label, 
                            ColNames.level,
                            ColNames.start_date, 
                            ColNames.end_date, 
                            ColNames.season]

    PARTITION_COLUMNS = [ColNames.dataset_id, 
                         ColNames.level, 
                         ColNames.label, 
                         ColNames.start_date, 
                         ColNames.end_date, 
                         ColNames.season]

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
