"""
Bronze MNO Network Topology Data module

Currently, only considers the "Cell Locations with Physical Properties" type
"""

from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ShortType, ByteType

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import ParquetInterface
from multimno.core.constants.columns import ColNames


class BronzeNetworkDataObject(PathDataObject):
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

    def __init__(
        self, spark: SparkSession, default_path: str, partition_columns: List[str] = None, mode: str = "overwrite"
    ) -> None:
        super().__init__(spark, default_path)
        self.interface = ParquetInterface()
        self.partition_columns = partition_columns
        self.mode = mode

    def write(self, path: str = None, partition_columns: List[str] = None, mode: str = None):
        if path is None:
            path = self.default_path
        if partition_columns is None:
            partition_columns = self.partition_columns
        if mode is None:
            mode = self.mode

        self.interface.write_from_interface(self.df, path, partition_columns)
