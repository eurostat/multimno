"""
Silver Event Data deduplication frequency quality metrics.
"""

from multimno.core.constants.columns import ColNames
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BinaryType,
    IntegerType,
    DateType,
)

from multimno.core.data_objects.data_object import ParquetDataObject


class SilverEventDataSyntacticQualityMetricsFrequencyDistribution(ParquetDataObject):
    """
    Class that models the Silver Event Data syntactic
    frequency quality metrics DataObject.
    """

    ID = "SilverEventDataSyntacticQualityMetricsFrequencyDistribution"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.user_id, BinaryType(), nullable=True),
            StructField(ColNames.initial_frequency, IntegerType(), nullable=False),
            StructField(ColNames.final_frequency, IntegerType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.date, ColNames.user_id_modulo]
