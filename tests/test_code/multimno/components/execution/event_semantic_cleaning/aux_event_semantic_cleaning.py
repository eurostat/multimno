import pytest
from configparser import ConfigParser
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    Row,
    StringType,
    StructField,
    StructType,
    IntegerType,
    TimestampType,
    FloatType,
    ShortType,
    ByteType,
)
import pyspark.sql.functions as F

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_event_flagged_data_object import SilverEventFlaggedDataObject
from multimno.core.data_objects.silver.silver_semantic_quality_metrics import SilverEventSemanticQualityMetrics
from tests.test_code.fixtures import spark_session as spark

fixtures = [spark]


@pytest.fixture
def expected_metrics(spark):
    # irrelevant timestamp
    timestamp = datetime.datetime.now()

    expected_data = [
        Row(
            **{
                ColNames.result_timestamp: timestamp,  # irrelevant
                ColNames.variable: ColNames.cell_id,
                ColNames.type_of_error: 3,
                ColNames.value: 2,
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        ),
        Row(
            **{
                ColNames.result_timestamp: timestamp,  # irrelevant
                ColNames.variable: ColNames.cell_id,
                ColNames.type_of_error: 4,
                ColNames.value: 10,
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        ),
        Row(
            **{
                ColNames.result_timestamp: timestamp,  # irrelevant
                ColNames.variable: ColNames.cell_id,
                ColNames.type_of_error: 0,
                ColNames.value: 11,
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        ),
        Row(
            **{
                ColNames.result_timestamp: timestamp,  # irrelevant
                ColNames.variable: ColNames.cell_id,
                ColNames.type_of_error: 2,
                ColNames.value: 1,
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        ),
        Row(
            **{
                ColNames.result_timestamp: timestamp,  # irrelevant
                ColNames.variable: ColNames.cell_id,
                ColNames.type_of_error: 1,
                ColNames.value: 1,
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        ),
    ]

    expected_data_df = spark.createDataFrame(expected_data, schema=SilverEventSemanticQualityMetrics.SCHEMA)
    return expected_data_df


@pytest.fixture
def expected_events(spark):
    cellids = [
        135347071883677,
        844911684991697,
        285720901775959,
        167106567403230,
        125400344428713,
        843918051560813,
        624029536387408,
        674835541016450,
    ]
    data = []
    modulo_value = 512
    hex_truncation_end = 12

    # Events that make a reference to a non existen cell ID
    data.extend(
        [
            (0, datetime.datetime(2023, 1, 3, 3, 0, 0), 214, cellids[0], None, None, None, 0),
            (0, datetime.datetime(2023, 1, 3, 3, 10, 30), 214, cellids[0], None, None, None, 0),
            (0, datetime.datetime(2023, 1, 3, 3, 20, 45), 214, 12345678901234, None, None, None, 1),  # non existent
        ]
    )

    # Events that make a reference to an invalid cell ID
    data.extend(
        [
            (1, datetime.datetime(2023, 1, 3, 10, 5, 0), 214, cellids[1], None, None, None, 0),
            (1, datetime.datetime(2023, 1, 3, 10, 12, 33), 214, cellids[3], None, None, None, 2),  # invalid
            (1, datetime.datetime(2023, 1, 3, 10, 26, 55), 214, cellids[0], None, None, None, 0),
        ]
    )

    # A) Two isolated events are too far away to be realistic. Both should be flagged as erroneous
    data.extend(
        [
            (2, datetime.datetime(2023, 1, 3, 12, 5, 0), 214, cellids[1], None, None, None, 4),  # madrid
            (2, datetime.datetime(2023, 1, 3, 12, 32, 33), 214, cellids[6], None, None, None, 4),  # barcelona
        ]
    )

    # B) Consecutive events, some clearly erroneous due to wrong cell locations, too far away. All flagged
    data.extend(
        [
            (3, datetime.datetime(2023, 1, 3, 11, 15, 0), 214, cellids[1], None, None, None, 4),  # madrid
            (3, datetime.datetime(2023, 1, 3, 11, 27, 33), 214, cellids[6], None, None, None, 3),  # barcelona
            (3, datetime.datetime(2023, 1, 3, 11, 42, 0), 214, cellids[2], None, None, None, 4),  # madrid
        ]
    )

    # C) Consecutive events where only one event is clearly erroneous, only that one is flagged
    data.extend(
        [
            (4, datetime.datetime(2023, 1, 3, 9, 30, 0), 214, cellids[1], None, None, None, 0),  # madrid
            (4, datetime.datetime(2023, 1, 3, 9, 45, 0), 214, cellids[0], None, None, None, 0),  # madrid
            (4, datetime.datetime(2023, 1, 3, 10, 00, 0), 214, cellids[1], None, None, None, 4),  # madrid
            (4, datetime.datetime(2023, 1, 3, 10, 30, 33), 214, cellids[6], None, None, None, 3),  # barcelona
            (4, datetime.datetime(2023, 1, 3, 11, 0, 0), 214, cellids[2], None, None, None, 4),  # madrid
            (4, datetime.datetime(2023, 1, 3, 11, 25, 0), 214, cellids[0], None, None, None, 0),  # madrid
        ]
    )

    # D) Consecutive events where several events are clearly erroneous probably due to the incorrect location data
    # of the network topology (wrong coordinates of the location of the cells), as it is too far away from other events
    # to be realistic.
    data.extend(
        [
            (5, datetime.datetime(2023, 1, 3, 9, 30, 0), 214, cellids[1], None, None, None, 0),  # madrid
            (5, datetime.datetime(2023, 1, 3, 9, 45, 0), 214, cellids[0], None, None, None, 0),  # madrid
            (5, datetime.datetime(2023, 1, 3, 10, 00, 0), 214, cellids[1], None, None, None, 4),  # madrid
            (5, datetime.datetime(2023, 1, 3, 10, 30, 33), 214, cellids[6], None, None, None, 4),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 10, 35, 33), 214, cellids[6], None, None, None, 0),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 10, 40, 33), 214, cellids[6], None, None, None, 4),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 11, 0, 0), 214, cellids[2], None, None, None, 4),  # madrid
            (5, datetime.datetime(2023, 1, 3, 11, 25, 0), 214, cellids[0], None, None, None, 0),  # madrid
        ]
    )

    event_df = [
        Row(
            **{
                ColNames.user_id: evdata[0],
                ColNames.timestamp: evdata[1],
                ColNames.mcc: evdata[2],
                ColNames.cell_id: evdata[3],
                ColNames.latitude: evdata[4],
                ColNames.longitude: evdata[5],
                ColNames.loc_error: evdata[6],
                ColNames.error_flag: evdata[7],
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        )
        for evdata in data
    ]

    # Temporary schema
    event_df = spark.createDataFrame(
        event_df,
        schema=StructType(
            [
                StructField(ColNames.user_id, IntegerType(), nullable=False),
                StructField(ColNames.timestamp, TimestampType(), nullable=False),
                StructField(ColNames.mcc, IntegerType(), nullable=False),
                StructField(ColNames.cell_id, StringType(), nullable=True),
                StructField(ColNames.latitude, FloatType(), nullable=True),
                StructField(ColNames.longitude, FloatType(), nullable=True),
                StructField(ColNames.loc_error, FloatType(), nullable=True),
                StructField(ColNames.error_flag, IntegerType(), nullable=False),
                StructField(ColNames.year, ShortType(), nullable=False),
                StructField(ColNames.month, ByteType(), nullable=False),
                StructField(ColNames.day, ByteType(), nullable=False),
            ]
        ),
    )

    event_df = (
        event_df.withColumn("hashed", F.sha2(F.col(ColNames.user_id).cast(StringType()), 256))
        .withColumn(ColNames.user_id, F.unhex(F.col("hashed")))
        .drop("hashed")
    )

    event_df = event_df.withColumn(
        ColNames.user_id_modulo,
        F.conv(F.substring(F.hex(F.col(ColNames.user_id)), 1, hex_truncation_end), 16, 10).cast("long")
        % F.lit(modulo_value).cast("bigint"),
    )

    event_df = spark.createDataFrame(event_df.rdd, schema=SilverEventFlaggedDataObject.SCHEMA)

    return event_df


def set_input_data(spark: SparkSession, config: ConfigParser):
    """"""
    network_test_data_path = config["Paths.Silver"]["network_data_silver"]
    event_test_data_path = config["Paths.Silver"]["event_data_silver_deduplicated"]

    network_data_list = [
        [-3.703464, 40.417163, 135347071883677, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [-3.696964, 40.419069, 844911684991697, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [-3.701318, 40.420054, 285720901775959, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [
            -3.698481,
            40.416471,
            167106567403230,
            datetime.datetime(2023, 1, 1, 0, 1, 0),
            datetime.datetime(2023, 1, 2, 14, 0, 0),
        ],
        [-2.129117, 40.067046, 125400344428713, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [-2.128266, 40.065068, 843918051560813, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [-3.553227, 44.757038, 624029536387408, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
        [2.169443, 41.381887, 674835541016450, datetime.datetime(2023, 1, 1, 0, 1, 0), None],
    ]
    cellids = [str(netdata[2]) for netdata in network_data_list]
    data = []
    modulo_value = 512
    hex_truncation_end = 12

    # Events that make a reference to a non existen cell ID
    data.extend(
        [
            (0, datetime.datetime(2023, 1, 3, 3, 0, 0), 214, cellids[0], None, None, None),
            (0, datetime.datetime(2023, 1, 3, 3, 10, 30), 214, cellids[0], None, None, None),
            (0, datetime.datetime(2023, 1, 3, 3, 20, 45), 214, 12345678901234, None, None, None),  # non existent
        ]
    )

    # Events that make a reference to an invalid cell ID
    data.extend(
        [
            (1, datetime.datetime(2023, 1, 3, 10, 5, 0), 214, cellids[1], None, None, None),
            (1, datetime.datetime(2023, 1, 3, 10, 12, 33), 214, cellids[3], None, None, None),  # invalid
            (1, datetime.datetime(2023, 1, 3, 10, 26, 55), 214, cellids[0], None, None, None),
        ]
    )

    # A) Two isolated events are too far away to be realistic. Both should be flagged as erroneous
    data.extend(
        [
            (2, datetime.datetime(2023, 1, 3, 12, 5, 0), 214, cellids[1], None, None, None),  # madrid
            (2, datetime.datetime(2023, 1, 3, 12, 32, 33), 214, cellids[6], None, None, None),  # barcelona
        ]
    )

    # B) Consecutive events, some clearly erroneous due to wrong cell locations, too far away. All flagged
    data.extend(
        [
            (3, datetime.datetime(2023, 1, 3, 11, 15, 0), 214, cellids[1], None, None, None),  # madrid
            (3, datetime.datetime(2023, 1, 3, 11, 27, 33), 214, cellids[6], None, None, None),  # barcelona
            (3, datetime.datetime(2023, 1, 3, 11, 42, 0), 214, cellids[2], None, None, None),  # madrid
        ]
    )

    # C) Consecutive events where only one event is clearly erroneous, only that one is flagged
    data.extend(
        [
            (4, datetime.datetime(2023, 1, 3, 9, 30, 0), 214, cellids[1], None, None, None),  # madrid
            (4, datetime.datetime(2023, 1, 3, 9, 45, 0), 214, cellids[0], None, None, None),  # madrid
            (4, datetime.datetime(2023, 1, 3, 10, 00, 0), 214, cellids[1], None, None, None),  # madrid
            (4, datetime.datetime(2023, 1, 3, 10, 30, 33), 214, cellids[6], None, None, None),  # barcelona
            (4, datetime.datetime(2023, 1, 3, 11, 0, 0), 214, cellids[2], None, None, None),  # madrid
            (4, datetime.datetime(2023, 1, 3, 11, 25, 0), 214, cellids[0], None, None, None),  # madrid
        ]
    )

    # D) Consecutive events where only one event is clearly erroneous, only that one is flagged
    data.extend(
        [
            (5, datetime.datetime(2023, 1, 3, 9, 30, 0), 214, cellids[1], None, None, None),  # madrid
            (5, datetime.datetime(2023, 1, 3, 9, 45, 0), 214, cellids[0], None, None, None),  # madrid
            (5, datetime.datetime(2023, 1, 3, 10, 00, 0), 214, cellids[1], None, None, None),  # madrid
            (5, datetime.datetime(2023, 1, 3, 10, 30, 33), 214, cellids[6], None, None, None),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 10, 35, 33), 214, cellids[6], None, None, None),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 10, 40, 33), 214, cellids[6], None, None, None),  # barcelona
            (5, datetime.datetime(2023, 1, 3, 11, 0, 0), 214, cellids[2], None, None, None),  # madrid
            (5, datetime.datetime(2023, 1, 3, 11, 25, 0), 214, cellids[0], None, None, None),  # madrid
        ]
    )
    network_df = [
        Row(
            **{
                ColNames.cell_id: netdata[2],
                ColNames.latitude: netdata[1],
                ColNames.longitude: netdata[0],
                ColNames.altitude: 200.0,
                ColNames.antenna_height: 100.0,
                ColNames.directionality: 1,
                ColNames.azimuth_angle: 50.0,
                ColNames.elevation_angle: 0.0,
                ColNames.horizontal_beam_width: 30.0,
                ColNames.vertical_beam_width: 30.0,
                ColNames.power: 200.0,
                ColNames.frequency: 2,
                ColNames.technology: "5G",
                ColNames.valid_date_start: netdata[3],
                ColNames.valid_date_end: netdata[4],
                ColNames.cell_type: "microcell",
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        )
        for netdata in network_data_list
    ]

    event_df = [
        Row(
            **{
                ColNames.user_id: evdata[0],
                ColNames.timestamp: evdata[1],
                ColNames.mcc: evdata[2],
                ColNames.cell_id: evdata[3],
                ColNames.latitude: evdata[4],
                ColNames.longitude: evdata[5],
                ColNames.loc_error: evdata[6],
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 3,
            }
        )
        for evdata in data
    ]

    network_df = spark.createDataFrame(network_df, schema=SilverNetworkDataObject.SCHEMA)

    event_df = spark.createDataFrame(
        event_df,
        schema=StructType(
            [
                StructField(ColNames.user_id, IntegerType(), nullable=False),
                StructField(ColNames.timestamp, TimestampType(), nullable=False),
                StructField(ColNames.mcc, IntegerType(), nullable=False),
                StructField(ColNames.cell_id, StringType(), nullable=True),
                StructField(ColNames.latitude, FloatType(), nullable=True),
                StructField(ColNames.longitude, FloatType(), nullable=True),
                StructField(ColNames.loc_error, FloatType(), nullable=True),
                StructField(ColNames.year, ShortType(), nullable=False),
                StructField(ColNames.month, ByteType(), nullable=False),
                StructField(ColNames.day, ByteType(), nullable=False),
            ]
        ),
    )

    event_df = (
        event_df.withColumn("hashed", F.sha2(F.col(ColNames.user_id).cast(StringType()), 256))
        .withColumn(ColNames.user_id, F.unhex(F.col("hashed")))
        .drop("hashed")
    )

    event_df = event_df.withColumn(
        ColNames.user_id_modulo,
        F.conv(F.substring(F.hex(F.col(ColNames.user_id)), 1, hex_truncation_end), 16, 10).cast("long")
        % F.lit(modulo_value).cast("bigint"),
    )

    event_df = spark.createDataFrame(event_df.rdd, schema=SilverEventDataObject.SCHEMA)

    # Write input data in test resources dir
    network_data = SilverNetworkDataObject(spark, network_test_data_path)
    network_data.df = network_df
    network_data.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])
    event_data = SilverEventDataObject(spark, event_test_data_path)
    event_data.df = event_df
    event_data.write()
