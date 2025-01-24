"""

"""

from pyspark.sql.types import StructType, StructField, StringType, ShortType, ByteType, LongType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverGeozonesGridMapDataObject(ParquetDataObject):
    """
    Class that models geographic and admin zones to grid mapping table.
    """

    ID = "SilverGeozonesGridMapDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, LongType(), nullable=False),
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

    PARTITION_COLUMNS = [ColNames.dataset_id, ColNames.year, ColNames.month, ColNames.day]
