""" """

from sedona.sql.types import GeometryType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from multimno.core.data_objects.data_object import GeoParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeBuildingsDataObject(GeoParquetDataObject):
    """
    Class that models the transportation network spatial data.
    """

    ID = "BronzeBuildingsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.category, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            # partition columns
            StructField(ColNames.quadkey, StringType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.quadkey]
