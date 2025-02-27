from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
    FloatType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverTourismOutboundNightsSpentDataObject(ParquetDataObject):
    """
    Data Object for monthly outbound tourism aggregations of nights spent per foreign country.
    """

    ID = "SilverTourismOutboundNightsSpentDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.time_period, StringType(), nullable=False),
            StructField(ColNames.country_of_destination, StringType(), nullable=False),
            StructField(ColNames.nights_spent, FloatType(), nullable=False),
            # partitioning columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month]
