from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
    FloatType,
    BooleanType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverTourismZoneDeparturesNightsSpentDataObject(ParquetDataObject):
    """
    Data Object for monthly tourism aggregations of nights spent and departures per geographical unit.
    """

    ID = "SilverTourismDeparturesNightsSpentDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.time_period, StringType(), nullable=False),
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.country_of_origin, StringType(), nullable=False),
            StructField(ColNames.is_overnight, BooleanType(), nullable=False),
            StructField(ColNames.nights_spent, FloatType(), nullable=False),
            StructField(ColNames.num_of_departures, FloatType(), nullable=False),
            # partitioning columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.level, ByteType(), nullable=False),
            StructField(ColNames.dataset_id, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.level, ColNames.dataset_id]
