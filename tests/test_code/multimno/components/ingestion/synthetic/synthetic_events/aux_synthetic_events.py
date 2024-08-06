import pytest
from configparser import ConfigParser
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql import functions as F

import datetime

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

from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.bronze.bronze_network_physical_data_object import (
    BronzeNetworkDataObject,
)

from multimno.core.data_objects.bronze.bronze_synthetic_diaries_data_object import (
    BronzeSyntheticDiariesDataObject,
)

from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)


from tests.test_code.test_common import TEST_GENERAL_CONFIG_PATH

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def set_input_data(spark: SparkSession, config: ConfigParser):
    """"""
    network_test_data_path = config["Paths.Bronze"]["network_data_bronze"]
    synthetic_diaries_test_data_path = config["Paths.Bronze"]["diaries_data_bronze"]

    network_data_list = [
        [
            -3.703464,
            40.417163,
            "135347071883677",
            datetime.datetime(2023, 1, 1, 0, 0, 0),
            None,
        ],
        [
            -3.696964,
            40.41800,
            "844911684991697",
            datetime.datetime(2023, 1, 1, 0, 0, 0),
            None,
        ],
        [
            -3.696964,
            40.419370,
            "512311684991831",
            datetime.datetime(2023, 1, 1, 0, 0, 0),
            None,
        ],
    ]

    network_list = [
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
                ColNames.range: 2000.0,
                ColNames.frequency: 2,
                ColNames.technology: "5G",
                ColNames.valid_date_start: str(netdata[3]),
                ColNames.valid_date_end: str(netdata[4]),
                ColNames.cell_type: "microcell",
                ColNames.year: 2023,
                ColNames.month: 1,
                ColNames.day: 1,
            }
        )
        for netdata in network_data_list
    ]

    network_df = spark.createDataFrame(network_list, schema=BronzeNetworkDataObject.SCHEMA)

    # ------- DIARIES DATA -------

    # 1) home near first cell
    # 2) movement near second cell
    # 3) work near third cell

    diaries_data_list = [
        [
            1,
            "stay",
            "home",
            -3.703463,
            40.417162,
            datetime.datetime(2023, 1, 1, 6, 0, 0),
            datetime.datetime(2023, 1, 1, 8, 0, 0),
            2023,
            1,
            1,
        ],
        [
            1,
            "move",
            "other",
            None,  # -3.696964,
            None,  # 40.417167,
            datetime.datetime(2023, 1, 1, 8, 0, 0),
            datetime.datetime(2023, 1, 1, 10, 0, 0),
            2023,
            1,
            1,
        ],
        [
            1,
            "stay",
            "work",
            -3.696964,
            40.419362,
            datetime.datetime(2023, 1, 1, 10, 0, 0),
            datetime.datetime(2023, 1, 1, 12, 0, 0),
            2023,
            1,
            1,
        ],
    ]

    diaries_list = [
        Row(
            **{
                ColNames.user_id: diarydata[0],
                ColNames.activity_type: diarydata[1],
                ColNames.stay_type: diarydata[2],
                ColNames.longitude: diarydata[3],
                ColNames.latitude: diarydata[4],
                ColNames.initial_timestamp: diarydata[5],
                ColNames.final_timestamp: diarydata[6],
                ColNames.year: diarydata[7],
                ColNames.month: diarydata[8],
                ColNames.day: diarydata[9],
            }
        )
        for diarydata in diaries_data_list
    ]

    TEMP_SCHEMA = StructType(
        [
            StructField(ColNames.user_id, IntegerType(), nullable=False),
            StructField(ColNames.activity_type, StringType(), nullable=True),
            StructField(ColNames.stay_type, StringType(), nullable=True),
            StructField(ColNames.longitude, FloatType(), nullable=True),
            StructField(ColNames.latitude, FloatType(), nullable=True),
            StructField(ColNames.initial_timestamp, TimestampType(), nullable=True),
            StructField(ColNames.final_timestamp, TimestampType(), nullable=True),
            # partition columns
            StructField(ColNames.year, ShortType(), nullable=False),
            StructField(ColNames.month, ByteType(), nullable=False),
            StructField(ColNames.day, ByteType(), nullable=False),
        ]
    )

    diaries_df = spark.createDataFrame(diaries_list, schema=TEMP_SCHEMA)

    modulo_value = 512
    hex_truncation_end = 12

    diaries_df = (
        diaries_df.withColumn("hashed", F.sha2(F.col(ColNames.user_id).cast(StringType()), 256))
        .withColumn(ColNames.user_id, F.unhex(F.col("hashed")))
        .drop("hashed")
    )

    diaries_df = diaries_df.withColumn(
        ColNames.user_id_modulo,
        F.conv(F.substring(F.hex(F.col(ColNames.user_id)), 1, hex_truncation_end), 16, 10).cast("long")
        % F.lit(modulo_value).cast("bigint"),
    )

    # Write input data in test resources dir
    diaries_data_object = BronzeSyntheticDiariesDataObject(spark, synthetic_diaries_test_data_path)
    diaries_data_object.df = diaries_df.orderBy(ColNames.initial_timestamp)
    diaries_data_object.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])

    network_data_object = BronzeNetworkDataObject(spark, network_test_data_path)
    network_data_object.df = network_df
    network_data_object.write(partition_columns=[ColNames.year, ColNames.month, ColNames.day])


@pytest.fixture
def expected_events(spark):

    df_list = [
        Row(
            user_id=None,
            timestamp=None,
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="135347071883677",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=None,
            timestamp=None,
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=None,
            timestamp=None,
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="844911684991697",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(b"\xdac\xb9;\xbd\xb2\xfe\x82\xc7\x18\xac\n6\xa6\xc3U"),
            timestamp="2023-01-01T07:27:06",
            mcc=1806459,
            mnc="3612704",
            plmn=None,
            cell_id="F854IU_62",
            latitude=1806286.0,
            longitude=1806242.0,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 06:03:01",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="135347071883677",
            latitude=40.41716384887695,
            longitude=-3.703463077545166,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 06:32:14",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="135347071883677",
            latitude=40.41716384887695,
            longitude=-3.703463077545166,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 06:36:16",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="135347071883677",
            latitude=40.41716384887695,
            longitude=-3.703463077545166,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 07:39:30",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="135347071883677",
            latitude=40.41716384887695,
            longitude=-3.703463077545166,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 10:08:17",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 10:08:17",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 10:22:10",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=38.39839172363281,
            longitude=-3.512115716934204,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 10:22:10",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 10:50:01",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 11:39:43",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=38.39839172363281,
            longitude=-3.512115716934204,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 11:39:43",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            user_id=bytearray(
                b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"
            ),
            timestamp="2023-01-01 11:44:37",
            mcc=214,
            mnc="01",
            plmn=None,
            cell_id="512311684991831",
            latitude=40.41936111450195,
            longitude=-3.6969640254974365,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    event_df = spark.createDataFrame(df_list, schema=BronzeEventDataObject.SCHEMA)

    return event_df
