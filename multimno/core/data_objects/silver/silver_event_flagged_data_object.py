"""
Silver MNO Event data module with flags computed in Semantic Checks
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
    BinaryType,
    IntegerType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventFlaggedDataObject(ParquetDataObject):
    """
    Class that models the cleaned MNO Event data, with flags computed
    in the semantic checks module.
    """

    ID = "SilverEventFlaggedDO"

    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.timestamp, TimestampType(), nullable=False),
            StructField(ColNames.mcc, IntegerType(), nullable=True),
            StructField(ColNames.mnc, StringType(), nullable=True),
            StructField(ColNames.plmn, IntegerType(), nullable=True),
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.loc_error, FloatType(), nullable=True),
            StructField(ColNames.error_flag, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo]
