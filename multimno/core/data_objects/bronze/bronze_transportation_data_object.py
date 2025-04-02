"""

"""

from sedona.sql.types import GeometryType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeTransportationDataObject(GeoParquetDataObject):
    """
    Class that models the transportation network spatial data.
    """

    ID = "BronzeTransportationDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.category, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.quadkey]
