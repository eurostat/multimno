"""
Silver Usual Environment Labels data object module
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    BinaryType,
    IntegerType,
    StringType,
    DateType,
    FloatType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverUsualEnvironmentLabelsDataObject(PathDataObject):
    """
    Class that models the Usual Environment Labels data object.
    """

    ID = "SilverUsualEnvironmentLabelsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.ue_label_rule, StringType(), nullable=False),
            StructField(ColNames.location_label_rule, StringType(), nullable=False),
            # partition columns
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = [
            ColNames.season,
            ColNames.start_date,
            ColNames.end_date,
            ColNames.user_id_modulo,
        ]

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
