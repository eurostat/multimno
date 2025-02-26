import datetime as dt
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    LongType,
    ShortType,
    ByteType,
    IntegerType,
    TimestampType,
)
from configparser import ConfigParser

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_daily_permanence_score_data_object import (
    SilverDailyPermanenceScoreDataObject,
)
from multimno.core.utils import calc_hashed_user_id, apply_schema_casting
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY

DPS_AUX_SCHEMA = StructType(
    [
        StructField(ColNames.user_id, StringType(), nullable=False),
        StructField(ColNames.dps, ArrayType(LongType()), nullable=False),
        StructField(ColNames.time_slot_initial_time, TimestampType(), nullable=False),
        StructField(ColNames.time_slot_end_time, TimestampType(), nullable=False),
        StructField(ColNames.year, ShortType(), nullable=False),
        StructField(ColNames.month, ByteType(), nullable=False),
        StructField(ColNames.day, ByteType(), nullable=False),
        StructField(ColNames.user_id_modulo, IntegerType(), nullable=False),
        StructField(ColNames.id_type, StringType(), nullable=True),
    ]
)


def get_input_daily_permanence_score():
    dt_format = "%Y-%m-%dT%H:%M:%S"
    DPS = [
        {
            "user_id": 1,
            "dps": [10005001000400, 10005001000300, 10006001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T00:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T01:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400, 10006001000300, 10005001000300, 10005001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T01:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T02:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400, 10006001000300, 10005001000300, 10005001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T02:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T03:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400, 10006001000300, 10005001000300, 10005001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T03:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T04:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400, 10005001000300, 10006001000300, 10005001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T04:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T05:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10005001000400, 10005001000300, 10006001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T05:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T06:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400, 10005001000300, 10006001000300, 10005001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T06:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T07:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10005001000400, 10005001000300, 10006001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T07:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T08:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T08:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T09:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T09:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T10:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T10:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T11:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T11:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T12:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T12:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T13:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T13:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T14:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T14:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T15:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T15:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T16:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T16:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T17:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T17:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T18:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T18:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T19:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T19:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T20:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T20:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T21:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T21:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T22:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T22:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T23:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 1,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T23:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-03T00:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 2,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T00:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T01:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 2,
            "dps": [10001001000300, 10002001000300, 10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T01:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T02:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T02:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T03:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T03:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T04:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T04:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T05:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T05:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T06:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T06:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T07:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T07:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T08:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T08:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T09:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 2,
            "dps": [10006001000400],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T09:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T10:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "grid",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T10:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T11:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T11:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T12:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T12:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T13:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T13:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T14:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T14:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T15:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T15:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T16:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T16:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T17:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T17:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T18:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T18:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T19:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T19:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T20:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T20:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T21:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T21:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T22:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T22:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-02T23:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
        {
            "user_id": 2,
            "dps": [-99],
            "time_slot_initial_time": dt.datetime.strptime("2023-01-02T23:00:00", dt_format),
            "time_slot_end_time": dt.datetime.strptime("2023-01-03T00:00:00", dt_format),
            "year": 2023,
            "month": 1,
            "day": 2,
            "user_id_modulo": 510,
            "id_type": "unknown",
        },
    ]

    rows = [Row(**x) for x in DPS]
    return rows


def get_expected_metrics():

    res_ts = dt.datetime.now()
    rows = [
        Row(
            result_timestamp=res_ts,
            num_unknown_devices=1,
            pct_unknown_devices=50.0,
            month=1,
            day=2,
            year=2023,
        ),
    ]

    return rows


def set_input_data(spark: SparkSession, config: ConfigParser):

    input_dps = spark.createDataFrame(get_input_daily_permanence_score(), schema=DPS_AUX_SCHEMA)

    dps_do = SilverDailyPermanenceScoreDataObject(
        spark, config.get(CONFIG_SILVER_PATHS_KEY, "daily_permanence_score_data_silver")
    )

    dps_do.df = apply_schema_casting(calc_hashed_user_id(input_dps), SilverDailyPermanenceScoreDataObject.SCHEMA)

    dps_do.write()
