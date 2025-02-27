"""
Silver Usual Environment Labels data object module
"""

from pyspark.sql.types import StructField, StructType, BinaryType, IntegerType, StringType, DateType, LongType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverUsualEnvironmentLabelsDataObject(ParquetDataObject):
    """
    Class that models the Usual Environment Labels data object.
    """

    ID = "SilverUsualEnvironmentLabelsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.user_id, BinaryType(), nullable=False),
            StructField(ColNames.grid_id, LongType(), nullable=False),
            StructField(ColNames.label, StringType(), nullable=False),
            StructField(ColNames.label_rule, StringType(), nullable=False),
            StructField(ColNames.id_type, StringType(), nullable=False),
            # partition columns
            StructField(ColNames.start_date, DateType(), nullable=False),
            StructField(ColNames.end_date, DateType(), nullable=False),
            StructField(ColNames.season, StringType(), nullable=False),
            StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.start_date, ColNames.end_date, ColNames.season, ColNames.user_id_modulo]
