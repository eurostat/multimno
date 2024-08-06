"""

"""

from sedona.sql import st_functions as STF
from sedona.sql.types import GeometryType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ShortType,
    ByteType,
)

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import GeoParquetInterface
from multimno.core.constants.columns import ColNames


class BronzeAdminUnitsDataObject(PathDataObject):
    """
    Class that models country polygons spatial data.
    """

    ID = "AdminUnitsDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.zone_id, StringType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=True),
            StructField(ColNames.level, ShortType(), nullable=True),
            StructField(ColNames.parent_id, StringType(), nullable=True),
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            # partition columns
            StructField(ColNames.dataset_id, StringType(), nullable=False),
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    def __init__(
        self, spark: SparkSession, default_path: str, partition_columns: "list[str]" = None, default_crs: int = 3035
    ) -> None:

        super().__init__(spark, default_path)
        self.interface: GeoParquetInterface = GeoParquetInterface()
        self.partition_columns = partition_columns

        self.default_crs = default_crs

    def read(self):

        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.SCHEMA)
        self.df = self.df.withColumn(ColNames.geometry, STF.ST_SetSRID((ColNames.geometry), F.lit(self.default_crs)))

    def write(self, path: str = None, partition_columns: "list[str]" = None):

        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
