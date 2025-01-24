"""

"""

from pyspark.sql.types import StructType, StructField, StringType, FloatType, ShortType, ByteType, LongType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverCellFootprintDataObject(ParquetDataObject):
    """ """

    ID = "SilverCellFootprintDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.grid_id, LongType(), nullable=True),
            # StructField(ColNames.valid_date_start, DateType(), nullable=True),
            # StructField(ColNames.valid_date_end, DateType(), nullable=True),
            StructField(ColNames.signal_dominance, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
