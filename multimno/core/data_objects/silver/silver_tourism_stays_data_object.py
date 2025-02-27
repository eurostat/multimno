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
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverTourismStaysDataObject(ParquetDataObject):
    """
    Data Object for daily tourism stays.
    """

    ID = "SilverTourismStaysDataObjectDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.time_segment_id, StringType(), nullable=False),
            StructField(ColNames.start_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.end_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.mcc, ShortType(), nullable=False),
            StructField(ColNames.mnc, StringType(), nullable=False),
            StructField(ColNames.plmn, StringType(), nullable=False),
            StructField(ColNames.zone_ids_list, ArrayType(StringType()), nullable=False),
            StructField(ColNames.zone_weights_list, ArrayType(FloatType()), nullable=False),
            StructField(ColNames.is_overnight, BooleanType(), nullable=False),
            # partitioning columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo, ColNames.dataset_id]
