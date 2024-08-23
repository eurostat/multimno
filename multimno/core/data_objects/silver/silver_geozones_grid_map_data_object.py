from typing import List

"""

"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ShortType, ByteType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class SilverGeozonesGridMapDataObject(PathDataObject):
    """
    Class that models geographic and admin zones to grid mapping table.
    """

    ID = "SilverGeozonesGridMapDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.hierarchical_id, StringType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            # StructField(ColNames.quadkey, StringType(), nullable=False),
        ]
    )

    def __init__(
        self,
        spark: SparkSession,
        default_path: str,
        partition_columns: List[str] = None,
    ) -> None:

        super().__init__(spark, default_path)
        self.interface: ParquetInterface = ParquetInterface()
        self.partition_columns = partition_columns

        # Clear path
        self.first_write = True

    def read(self):

        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.SCHEMA)

    def write(self, path: str = None, partition_columns: List[str] = None):

        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
