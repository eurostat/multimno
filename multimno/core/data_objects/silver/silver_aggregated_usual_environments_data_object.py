"""
Silver Aggregated Usual Environments data object module
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    DateType,
    FloatType,
    LongType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverAggregatedUsualEnvironmentsDataObject(ParquetDataObject):
    """
    Class that models the Aggregated Usual Environment data object.
    """

    ID = "SilverAggregatedUsualEnvironmentsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.grid_id, LongType(), nullable=False),
            StructField(ColNames.weighted_device_count, FloatType(), nullable=False),
            # partition columns
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.label, ColNames.start_date, ColNames.end_date, ColNames.season]
