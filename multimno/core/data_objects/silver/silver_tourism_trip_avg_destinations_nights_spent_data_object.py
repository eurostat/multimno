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


class SilverTourismTripAvgDestinationsNightsSpentDataObject(ParquetDataObject):
    """
    Data Object for monthly tourism aggregations of average number of destinations and nights spent per trip.
    """

    ID = "SilverTourismTripAvgDestinationsNightsSpentDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.time_period, StringType(), nullable=False),
            StructField(ColNames.country_of_origin, StringType(), nullable=False),
            StructField(ColNames.avg_destinations, FloatType(), nullable=False),
            StructField(ColNames.avg_nights_spent_per_destination, FloatType(), nullable=False),
            # partitioning columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.level, ColNames.dataset_id]
