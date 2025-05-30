"""

"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverGroupToTileDataObject(ParquetDataObject):
    """ """

    ID = "SilverGroupToTileDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.group_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, IntegerType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = []
