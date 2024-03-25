"""

"""

from sedona.sql import st_functions as STF
from sedona.sql.types import GeometryType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import GeoParquetInterface
from multimno.core.constants.columns import ColNames


class SilverGridDataObject(PathDataObject):
    """
    Class that models operational grid.
    """

    ID = "SilverGridDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.geometry, GeometryType(), nullable=False),
            StructField(ColNames.grid_id, StringType(), nullable=False),
            StructField(ColNames.elevation, FloatType(), nullable=True),
            StructField(ColNames.land_use, StringType(), nullable=True),
            StructField(ColNames.prior_probability, FloatType(), nullable=True),
        ]
    )

    MANDATORY_COLUMNS = [ColNames.grid_id, ColNames.geometry]
    OPTIONAL_COLUMNS = [ColNames.elevation, ColNames.land_use, ColNames.prior_probability]

    def __init__(
        self, spark: SparkSession, default_path: str, partition_columns: list[str] = None, default_crs: int = 3035
    ) -> None:

        super().__init__(spark, default_path)
        self.interface: GeoParquetInterface = GeoParquetInterface()
        self.partition_columns = partition_columns

        # Clear path
        self.first_write = True
        self.default_crs = default_crs

    def read(self):

        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.SCHEMA)
        self.df = self.df.withColumn(ColNames.geometry, STF.ST_SetSRID((ColNames.geometry), F.lit(self.default_crs)))

    def write(self, path: str = None, partition_columns: list[str] = None):

        if partition_columns is None:
            partition_columns = self.partition_columns
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
