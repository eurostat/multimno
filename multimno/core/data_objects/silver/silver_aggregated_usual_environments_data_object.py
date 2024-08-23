from typing import List

"""
Silver Aggregated Usual Environments data object module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DateType,
    FloatType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverAggregatedUsualEnvironmentsDataObject(PathDataObject):
    """
    Class that models the Aggregated Usual Environment data object.
    """

    ID = "SilverAggregatedUsualEnvironmentsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.weighted_device_count, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: List[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = partition_columns

    def write(self, path: str = None, partition_columns: List[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
