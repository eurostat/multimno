"""
Silver present population estimatation per zone data object
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    StringType,
    ByteType,
    ShortType,
    TimestampType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverPresentPopulationZoneDataObject(ParquetDataObject):
    """
    Estimation of the population present at a given time at the level of some zoning system.
    """

    ID = "SilverPresentPopulationZoneDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.population, FloatType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    VALUE_COLUMNS = [ColNames.population]

    AGGREGATION_COLUMNS = [
        ColNames.zone_id,
        ColNames.timestamp,
        ColNames.dataset_id,
        ColNames.level,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    PARTITION_COLUMNS = [ColNames.dataset_id, ColNames.level, ColNames.year, ColNames.month, ColNames.day]
