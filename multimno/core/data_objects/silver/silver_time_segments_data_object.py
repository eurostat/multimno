"""

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
    ArrayType,
    BooleanType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverTimeSegmentsDataObject(ParquetDataObject):
    """
    Class that models the cleaned MNO Event data.
    """

    ID = "SilverTimeSegmentsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.time_segment_id, StringType(), nullable=False),
            StructField(ColNames.start_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.end_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.mcc, ShortType(), nullable=False),
            StructField(ColNames.mnc, StringType(), nullable=False),
            StructField(ColNames.plmn, IntegerType(), nullable=True),
            StructField(ColNames.cells, ArrayType(StringType()), nullable=False),
            StructField(ColNames.state, StringType(), nullable=False),
            StructField(ColNames.is_last, BooleanType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.year,
        ColNames.month,
        ColNames.day,
        ColNames.user_id_modulo,
    ]
