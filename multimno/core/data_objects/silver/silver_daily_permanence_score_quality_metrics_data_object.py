"""
Silver Daily Permanence Score Quality Metric data module
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    LongType,
    ShortType,
    ByteType,
    FloatType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverDailyPermanenceScoreQualityMetrics(ParquetDataObject):
    """
    Class that models the Daily Permanence Score quality metrics data.
    """

    ID = "SilverDailyPermanenceScoreQualityMetricsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.num_unknown_devices, LongType(), nullable=False),
            StructField(ColNames.pct_unknown_devices, FloatType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
            # partition column
            StructField(ColNames.year, ShortType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year]
