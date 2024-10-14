"""

"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, ShortType, ByteType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellIntersectionGroupsDataObject(ParquetDataObject):
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

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
