"""
Silver MNO Network Topology Data module
"""

from pyspark.sql.types import (
    StructField,
    StructType,
    FloatType,
    IntegerType,
    StringType,
    ByteType,
    ShortType,
    TimestampType,
)

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class SilverNetworkDataObject(ParquetDataObject):
    """
    Class that models the clean MNO Network Topology Data, based on the physical
    properties of the cells.
    """

    ID = "SilverNetworkDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=False),
            StructField(ColNames.latitude, FloatType(), nullable=False),
            StructField(ColNames.longitude, FloatType(), nullable=False),
            StructField(ColNames.altitude, FloatType(), nullable=True),
            StructField(ColNames.antenna_height, FloatType(), nullable=True),
            StructField(ColNames.directionality, IntegerType(), nullable=False),
            StructField(ColNames.azimuth_angle, FloatType(), nullable=True),
            StructField(ColNames.elevation_angle, FloatType(), nullable=True),
            StructField(ColNames.horizontal_beam_width, FloatType(), nullable=True),
            StructField(ColNames.vertical_beam_width, FloatType(), nullable=True),
            StructField(ColNames.power, FloatType(), nullable=True),
            StructField(ColNames.range, FloatType(), nullable=True),
            StructField(ColNames.frequency, IntegerType(), nullable=True),
            StructField(ColNames.technology, StringType(), nullable=True),
            StructField(ColNames.valid_date_start, TimestampType(), nullable=True),
            StructField(ColNames.valid_date_end, TimestampType(), nullable=True),
            StructField(ColNames.cell_type, StringType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
