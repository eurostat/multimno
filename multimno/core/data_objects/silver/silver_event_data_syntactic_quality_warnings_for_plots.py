"""
Silver Event Data quality warning for plots table.
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


class SilverEventDataSyntacticQualityWarningsForPlots(ParquetDataObject):
    """
    Class that store data to plot raw, clean data sizez and error rate.
    """

    ID = "SilverEventDataSyntacticQualityWarningsForPlots"
    SCHEMA = StructType(
        [
            StructField(ColNames.type_of_qw, StringType(), nullable=False),
            StructField(ColNames.lookback_period, StringType(), nullable=False),
            StructField(ColNames.daily_value, FloatType(), nullable=False),
            StructField(ColNames.average, FloatType(), nullable=True),
            StructField(ColNames.LCL, FloatType(), nullable=True),
            StructField(ColNames.UCL, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.date, DateType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.date]
