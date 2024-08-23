from typing import List
"""

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, ShortType, ByteType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverCellIntersectionGroupsDataObject(PathDataObject):
    """ """

    ID = "SilverCellIntersectionGroupsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.group_id, StringType(), nullable=True),
            StructField(ColNames.cells, ArrayType(StringType()), nullable=True),
            StructField(ColNames.group_size, IntegerType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.group_id,
        ColNames.cells,
        ColNames.group_size,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

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
