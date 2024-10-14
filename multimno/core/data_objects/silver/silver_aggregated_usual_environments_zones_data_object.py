"""
Silver usual environments estimatation per zone data object
"""

from pyspark.sql.types import StructField, StructType, FloatType, StringType, DateType, ByteType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverAggregatedUsualEnvironmentsZonesDataObject(ParquetDataObject):
    """
    Estimation of the population present at a given time at the level of some zoning system.
    """

    ID = "SilverAggregatedUsualEnvironmentsZonesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.weighted_device_count, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
        ]
    )

    VALUE_COLUMNS = [ColNames.weighted_device_count]

    AGGREGATION_COLUMNS = [
        ColNames.zone_id,
        ColNames.dataset_id,
        ColNames.label,
        ColNames.level,
        ColNames.start_date,
        ColNames.end_date,
        ColNames.season,
    ]

    PARTITION_COLUMNS = [
        ColNames.dataset_id,
        ColNames.level,
        ColNames.label,
        ColNames.start_date,
        ColNames.end_date,
        ColNames.season,
    ]
