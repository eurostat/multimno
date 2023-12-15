from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BinaryType

from core.data_objects.data_object import PathDataObject
from core.io_interface import ParquetInterface
from common.constants.columns import ColNames


class BronzeEventDataObject(PathDataObject):
    ID = "BronzeEventDO"
    SCHEMA = StructType([
        StructField(ColNames.user_id, BinaryType(), nullable=True),
        StructField(ColNames.timestamp, StringType(), nullable=True),
        StructField(ColNames.mcc, IntegerType(), nullable=True),
        StructField(ColNames.cell_id, StringType(), nullable=True),
        StructField(ColNames.latitude, FloatType(), nullable=True),
        StructField(ColNames.longitude, FloatType(), nullable=True),
        StructField(ColNames.loc_error, FloatType(), nullable=True)
    ])

    def __init__(self, spark: SparkSession, default_path: str, partition_columns: list[str] = None) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = partition_columns

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns

        self.interface.write_from_interface(self.df, path, partition_columns)
