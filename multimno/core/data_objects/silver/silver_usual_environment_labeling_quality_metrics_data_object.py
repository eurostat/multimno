"""
Silver Usual Environment Labeling Quality Metrics data object module
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    LongType,
    StringType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverUsualEnvironmentLabelingQualityMetricsDataObject(ParquetDataObject):
    """
    Class that models the Usual Environment Labeling Quality Metrics data object.
    """

    ID = "SilverUsualEnvironmentLabelingQualityMetricsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.labeling_quality_metric, StringType(), nullable=False),
            StructField(ColNames.labeling_quality_count, LongType(), nullable=False),
            StructField(ColNames.labeling_quality_min, LongType(), nullable=False),
            StructField(ColNames.labeling_quality_max, LongType(), nullable=False),
            StructField(ColNames.labeling_quality_avg, LongType(), nullable=False),
            # partition columns
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.start_date, ColNames.end_date]
