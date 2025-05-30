"""
Silver Daily Permanence Score data module
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    BinaryType,
    StringType,
    ArrayType,
    ShortType,
    ByteType,
    IntegerType,
    LongType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverDailyPermanenceScoreDataObject(ParquetDataObject):
    """
    Class that models the Daily Permanence Score data.
    """

    ID = "SilverDailyPermanenceScoreDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.time_slot_initial_time, TimestampType(), nullable=False),
            StructField(ColNames.time_slot_end_time, TimestampType(), nullable=False),
            StructField(ColNames.dps, ArrayType(StringType()), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.year,
        ColNames.month,
        ColNames.day,
        ColNames.user_id_modulo,
        ColNames.id_type,
    ]
