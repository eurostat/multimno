"""
Silver Usual Environment Labeling Quality Metrics data object module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    DateType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverUsualEnvironmentLabelingQualityMetricsDataObject(PathDataObject):
    """
    Class that models the Usual Environment Labeling Quality Metrics data object.
    """

    ID = "SilverUsualEnvironmentLabelingQualityMetricsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.labeling_quality_metric, StringType(), nullable=False),
            StructField(ColNames.labeling_quality_count, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [
            ColNames.season,
            ColNames.start_date,
            ColNames.end_date,
        ]

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
