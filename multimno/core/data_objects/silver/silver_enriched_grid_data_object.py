"""

"""

from sedona.sql.types import GeometryType
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverEnrichedGridDataObject(GeoParquetDataObject):
    """
    Class that models operational grid.
    """

    ID = "SilverEnrichedGridDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.elevation, FloatType(), nullable=True),
            StructField(ColNames.prior_probability, FloatType(), nullable=True),
            StructField(ColNames.ple_coefficient, FloatType(), nullable=True),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [ColNames.grid_id, ColNames.geometry]
    OPTIONAL_COLUMNS = [ColNames.elevation, ColNames.ple_coefficient, ColNames.prior_probability]

    PARTITION_COLUMNS = [ColNames.quadkey]
