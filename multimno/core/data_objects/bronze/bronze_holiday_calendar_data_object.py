"""
Bronze Calendar Information Data Object
Contains the national holidays of each country
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, DateType, StringType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class BronzeHolidayCalendarDataObject(PathDataObject):
    """
    Class that models the Calendar information regarding national holidays
    and regular days.
    """

    ID = "BronzeHolidayCalendarInfoDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.date, DateType(), nullable=False),
            StructField(ColNames.name, StringType(), nullable=False),
        ]
    )

    def __init__(self, spark: SparkSession, default_path: str) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()

    def write(self, path: str = None, partition_columns: list[str] = None):
        if path is None:
            path = self.default_path

        self.interface.write_from_interface(self.df, path, partition_columns)
