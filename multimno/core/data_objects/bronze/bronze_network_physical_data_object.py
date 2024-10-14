"""
Bronze MNO Network Topology Data module

Currently, only considers the "Cell Locations with Physical Properties" type
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ShortType, ByteType

from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeNetworkDataObject(ParquetDataObject):
    """
    Class that models the RAW MNO Network Topology Data, based on the physical
    properties of the cells.
    """

    ID = "BronzeNetworkDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.cell_id, StringType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.altitude, FloatType(), nullable=True),
            StructField(ColNames.antenna_height, FloatType(), nullable=True),
            StructField(ColNames.directionality, IntegerType(), nullable=True),
            StructField(ColNames.azimuth_angle, FloatType(), nullable=True),
            StructField(ColNames.elevation_angle, FloatType(), nullable=True),
            StructField(ColNames.horizontal_beam_width, FloatType(), nullable=True),
            StructField(ColNames.vertical_beam_width, FloatType(), nullable=True),
            StructField(ColNames.power, FloatType(), nullable=True),
            StructField(ColNames.range, FloatType(), nullable=True),
            StructField(ColNames.frequency, IntegerType(), nullable=True),
            StructField(ColNames.technology, StringType(), nullable=True),
            StructField(ColNames.valid_date_start, StringType(), nullable=True),
            StructField(ColNames.valid_date_end, StringType(), nullable=True),
            StructField(ColNames.cell_type, StringType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=True),
            StructField(ColNames.month, ByteType(), nullable=True),
            StructField(ColNames.day, ByteType(), nullable=True),
        ]
    )
    MANDATORY_COLUMNS = [ColNames.cell_id, ColNames.latitude, ColNames.longitude]

    OPTIONAL_COLUMNS = [
        ColNames.altitude,
        ColNames.antenna_height,
        ColNames.directionality,
        ColNames.azimuth_angle,
        ColNames.elevation_angle,
        ColNames.horizontal_beam_width,
        ColNames.vertical_beam_width,
        ColNames.power,
        ColNames.range,
        ColNames.frequency,
        ColNames.technology,
        ColNames.valid_date_start,
        ColNames.valid_date_end,
        ColNames.cell_type,
    ]

    PARTITION_COLUMNS = [ColNames.year, ColNames.month, ColNames.day]
