"""
Silver Event Data quality metrics.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    ShortType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventDataSyntacticQualityMetricsByColumn(ParquetDataObject):
    """
    Class that models the Silver Event Data quality metrics DataObject.
    """

    ID = "SilverEventDataSyntacticQualityMetricsByColumn"
    SCHEMA = StructType(
        [
            StructField(ColNames.result_timestamp, TimestampType(), nullable=False),
            StructField(ColNames.variable, StringType(), nullable=True),
            StructField(ColNames.type_of_error, ShortType(), nullable=True),
            StructField(ColNames.value, IntegerType(), nullable=False),
            # partition columns
            StructField(ColNames.date, DateType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.date]
