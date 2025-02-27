"""

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    ShortType,
    ByteType,
    BinaryType,
    FloatType,
    BooleanType,
    ArrayType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverTourismTripDataObject(ParquetDataObject):
    """
    Data Object for tourism trips.
    """

    ID = "SilverTourismTripsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.trip_id, StringType(), nullable=False),
            StructField(ColNames.trip_start_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.time_segment_ids_list, ArrayType(StringType()), nullable=False),
            StructField(ColNames.is_trip_finished, BooleanType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.user_id,
        ColNames.user_id_modulo,
        ColNames.trip_id,
        ColNames.trip_start_timestamp,
        ColNames.time_segment_ids_list,
        ColNames.year,
        ColNames.month,
        ColNames.dataset_id,
    ]

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.user_id_modulo, ColNames.dataset_id]
