"""
Silver MNO Event data module with flags computed in Semantic Checks
"""

from multimno.core.data_objects.silver.silver_event_flagged_data_object import SilverEventFlaggedDataObject
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
)

from multimno.core.constants.columns import ColNames


class EventCacheDataObject(SilverEventFlaggedDataObject):
    """
    Class that models the cleaned MNO Event data, with flags computed
    in the semantic checks module.
    """

    ID = "EventCacheDO"

    # SCHEMA and partition columns depend on the semantic event data
    SCHEMA = StructType(
        SilverEventFlaggedDataObject.SCHEMA.fields
        + [StructField(ColNames.is_last_event, BooleanType(), nullable=False)]
    )

    PARTITION_COLUMNS = SilverEventFlaggedDataObject.PARTITION_COLUMNS + [ColNames.is_last_event]
