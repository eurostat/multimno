"""
Silver Internal Migration data object module
"""

from pyspark.sql.types import StructField, StructType, StringType, DateType, ByteType, TimestampType, LongType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverInternalMigrationQualityMetricsDataObject(ParquetDataObject):
    """
    Class that models the Internal Migration data object.
    """

    ID = "SilverInternalMigrationQualityMetricsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.previous_home_users, LongType(), nullable=False),
            StructField(ColNames.new_home_users, LongType(), nullable=False),
            StructField(ColNames.common_home_users, LongType(), nullable=False),
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.start_date_previous, DateType(), nullable=False),
            StructField(ColNames.end_date_previous, DateType(), nullable=False),
            StructField(ColNames.season_previous, StringType(), nullable=False),
            StructField(ColNames.start_date_new, DateType(), nullable=False),
            StructField(ColNames.end_date_new, DateType(), nullable=False),
            StructField(ColNames.season_new, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [
        ColNames.dataset_id,
        ColNames.level,
        ColNames.start_date_previous,
        ColNames.end_date_previous,
        ColNames.season_previous,
        ColNames.start_date_new,
        ColNames.end_date_new,
        ColNames.season_new,
    ]
