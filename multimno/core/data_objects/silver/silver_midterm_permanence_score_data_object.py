"""
Silver Mid Term Permanence Score data object module
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    BinaryType,
    IntegerType,
    StringType,
    ShortType,
    ByteType,
    FloatType,
    LongType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverMidtermPermanenceScoreDataObject(ParquetDataObject):
    """
    Class that models the Midterm Permanence Score data object.
    """

    ID = "SilverMidtermPermanenceScoreDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.grid_id, LongType(), nullable=False),
            StructField(ColNames.mps, IntegerType(), nullable=False),
            StructField(ColNames.frequency, IntegerType(), nullable=False),
            StructField(ColNames.regularity_mean, FloatType(), nullable=True),
            StructField(ColNames.regularity_std, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day_type, StringType(), nullable=False),
            StructField(ColNames.time_interval, StringType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.year,
        ColNames.month,
        ColNames.day_type,
        ColNames.time_interval,
        ColNames.id_type,
        ColNames.user_id_modulo,
    ]
