from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
    FloatType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellDistanceDataObject(ParquetDataObject):
    """
    Data Object for distances between cells.
    Identifies for each (cell, date) the distance to another cell.
    """

    ID = "SilverCellDistanceDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id_a, StringType(), nullable=True),
            StructField(ColNames.cell_id_b, StringType(), nullable=True),
            StructField(ColNames.distance, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.cell_id_a,
        ColNames.cell_id_b,
        ColNames.distance,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
