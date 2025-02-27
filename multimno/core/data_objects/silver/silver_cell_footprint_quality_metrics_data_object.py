"""

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
    TimestampType,
    LongType,
    FloatType,
)
from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellFootprintQualityMetrics(ParquetDataObject):
    """ """

    ID = "SilverCellFootprintQualityMetricsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.cell_id, StringType(), nullable=False),
            StructField(ColNames.number_of_events, LongType(), nullable=False),
            StructField(ColNames.percentage_total_events, FloatType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
