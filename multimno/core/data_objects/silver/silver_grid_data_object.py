"""

"""

from sedona.sql.types import GeometryType
from pyspark.sql.types import StructType, StructField, LongType, StringType

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverGridDataObject(GeoParquetDataObject):
    """
    Class that models operational grid.
    """

    ID = "SilverGridDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            StructField(ColNames.grid_id, LongType(), nullable=False),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [ColNames.grid_id, ColNames.geometry]

    PARTITION_COLUMNS = [ColNames.quadkey]
