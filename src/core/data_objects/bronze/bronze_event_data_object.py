from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BinaryType

from core.data_objects.data_object import PathDataObject
from core.io_interface import PathInterface
from common.constants.columns import ColNames

class BronzeEventDataObject(PathDataObject):
    ID = "BronzeEventDO"
    SCHEMA = StructType([
        StructField(ColNames.user_id, BinaryType(), nullable=False),
        StructField(ColNames.timestamp, StringType(), nullable=False),
        StructField(ColNames.mcc, IntegerType(), nullable=False),
        StructField(ColNames.cell_id, StringType(), nullable=False),
        StructField(ColNames.latitude, FloatType(), nullable=True),
        StructField(ColNames.longitude, FloatType(), nullable=True),
        StructField(ColNames.loc_error, FloatType(), nullable=True)
    ])

    def __init__(self, spark: SparkSession, default_path: str, interface: PathInterface) -> None:
        super().__init__(spark, default_path)
        self.interface = interface



