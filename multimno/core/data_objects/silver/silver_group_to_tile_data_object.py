"""

"""

from pyspark.sql.types import StructType, StructField, StringType, ShortType, ByteType, IntegerType
from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverGroupToTileDataObject(ParquetDataObject):
    """ """

    ID = "SilverGroupToTileDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.group_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, IntegerType(), nullable=True),
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
