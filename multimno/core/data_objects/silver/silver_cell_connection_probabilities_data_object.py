"""
Cell connection probabilities.
"""

from pyspark.sql.types import StructType, StructField, StringType, FloatType, ShortType, ByteType, IntegerType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellConnectionProbabilitiesDataObject(ParquetDataObject):
    """ """

    ID = "SilverCellConnectionProbabilitiesDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, IntegerType(), nullable=True),
            StructField(ColNames.cell_connection_probability, FloatType(), nullable=True),
            StructField(ColNames.posterior_probability, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [
        ColNames.cell_id,
        ColNames.grid_id,
        ColNames.valid_date_start,
        ColNames.valid_date_end,
        ColNames.cell_connection_probability,
        ColNames.posterior_probability,
        ColNames.year,
        ColNames.month,
        ColNames.day,
    ]

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
