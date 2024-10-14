"""
Bronze Calendar Information Data Object
Contains the national holidays of each country
"""

from pyspark.sql.types import StructField, StructType, DateType, StringType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeHolidayCalendarDataObject(ParquetDataObject):
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
    PARTITION_COLUMNS = []
