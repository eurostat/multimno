from typing import List

"""
Cell connection probabilities.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ShortType, ByteType, DateType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverCellConnectionProbabilitiesDataObject(PathDataObject):
    """ """

    ID = "SilverCellConnectionProbabilitiesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, StringType(), nullable=True),
            # StructField(ColNames.valid_date_start, DateType(), nullable=True),
            # StructField(ColNames.valid_date_end, DateType(), nullable=True),
            StructField(ColNames.cell_connection_probability, FloatType(), nullable=True),
            StructField(ColNames.posterior_probability, FloatType(), nullable=True),
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
        ColNames.cell_connection_probability,
        ColNames.posterior_probability,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    def __init__(
        self, spark: SparkSession, default_path: str, partition_columns: list[str] = None, mode: str = "overwrite"
    ) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
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
