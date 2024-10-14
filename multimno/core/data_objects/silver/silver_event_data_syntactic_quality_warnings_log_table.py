"""
Silver Event Data Quality Warning log table.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEventDataSyntacticQualityWarningsLogTable(ParquetDataObject):
    """
    Class that stores information about Event Quallity Warnings
    """

    ID = "SilverEventDataSyntacticQualityWarningsLogTable"
    SCHEMA = StructType(
        [
            StructField(ColNames.measure_definition, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.condition_value, FloatType(), nullable=True),
            StructField(ColNames.condition, StringType(), nullable=False),
            StructField(ColNames.warning_text, StringType(), nullable=False),
            # partition columns
            StructField(ColNames.date, DateType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.date]
