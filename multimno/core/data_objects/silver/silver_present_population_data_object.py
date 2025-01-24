"""
Silver present population estimatation per grid data object
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    ByteType,
    ShortType,
    TimestampType,
    LongType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverPresentPopulationDataObject(ParquetDataObject):
    """
    Estimation of the population present at a given time at the grid tile level.
    """

    ID = "SilverPresentPopulationDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, LongType(), nullable=False),
            StructField(ColNames.population, FloatType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
