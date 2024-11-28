"""

"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, ShortType, ByteType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellIntersectionGroupsDataObject(ParquetDataObject):
    """
    Data Object for cell intersection groups.
    Identifies for each (cell, date) the list of other cells
    that are considered as intersecting (having sufficiently overlapping coverage area).
    """

    ID = "SilverCellIntersectionGroupsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.overlapping_cell_ids, ArrayType(StringType()), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.cell_id,
        ColNames.overlapping_cell_ids,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
