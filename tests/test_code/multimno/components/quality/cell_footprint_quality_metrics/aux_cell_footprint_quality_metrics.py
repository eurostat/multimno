import datetime as dt
from pyspark.sql import Row
from pyspark.sql import SparkSession
from configparser import ConfigParser
from hashlib import sha256

from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject

from multimno.core.settings import CONFIG_SILVER_PATHS_KEY


def get_input_footprint():
    rows = [
        Row(
            cell_id="11111111111111",
            grid_id=12345671234567,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="11111111111111",
            grid_id=12345671234568,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="11111111111111",
            grid_id=12345671234569,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="22222222222222",
            grid_id=12345671234567,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="22222222222222",
            grid_id=12345671234568,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="33333333333333",
            grid_id=12345671234567,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="33333333333333",
            grid_id=12345671234597,
            signal_dominance=0.392,
            year=2023,
            month=1,
            day=1,
        ),
    ]
    return rows


def get_input_events():
    date_format = "%Y-%m-%d %H:%M:%S"

    rows = [
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 15:13:00", date_format),
            mcc=154,
            mnc="01",
            plmn=None,
            cell_id="11111111111111",
            latitude=26.129932,
            longitude=12.52221,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=1,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 15:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=None,
            cell_id="11111111111111",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"2").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 15:08:00", date_format),
            mcc=123,
            mnc="01",
            plmn=None,
            cell_id="22222222222222",
            latitude=0.0,
            longitude=0.0,
            loc_error=100.0,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 16:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=12301,
            cell_id="22222222222222",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 16:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=12301,
            cell_id="33333333333333",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 16:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=12301,
            cell_id="44444444444444",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 16:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=12301,
            cell_id="44444444444444",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
        Row(
            user_id=sha256(b"1").digest(),
            timestamp=dt.datetime.strptime("2023-01-01 16:00:00", date_format),
            mcc=123,
            mnc="01",
            plmn=12301,
            cell_id="55555555555555",
            latitude=None,
            longitude=None,
            loc_error=None,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=0,
        ),
    ]

    return rows


def get_input_network():
    rows = [
        Row(
            cell_id="11111111111111",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=dt.datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=dt.datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="22222222222222",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=dt.datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=dt.datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="33333333333333",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=dt.datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=dt.datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="44444444444444",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=dt.datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=dt.datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="55555555555555",
            latitude=50.80180,
            longitude=4.43345,
            altitude=150.0,
            antenna_height=30.0,
            directionality=0,
            azimuth_angle=None,
            elevation_angle=None,
            horizontal_beam_width=None,
            vertical_beam_width=None,
            power=2.0,
            range=5000.0,
            frequency=1281,
            technology="LTE",
            valid_date_start=dt.datetime.strptime("2023-01-01T01:54:45", "%Y-%m-%dT%H:%M:%S"),
            valid_date_end=dt.datetime.strptime("2023-02-01T06:58:50", "%Y-%m-%dT%H:%M:%S"),
            cell_type=None,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    return rows


def get_expected_metrics():

    res_ts = dt.datetime.now()
    rows = [
        Row(
            result_timestamp=res_ts,
            cell_id=44444444444444,
            number_of_events=2,
            percentage_total_events=25.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            result_timestamp=res_ts,
            cell_id=55555555555555,
            number_of_events=1,
            percentage_total_events=12.5,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    return rows


def set_input_data(spark: SparkSession, config: ConfigParser):

    input_network = get_input_network()
    input_events = get_input_events()
    input_footprint = get_input_footprint()

    network_do = SilverNetworkDataObject(spark, config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver"))
    events_do = SilverEventDataObject(spark, config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver"))
    footprint_do = SilverCellFootprintDataObject(
        spark, config.get(CONFIG_SILVER_PATHS_KEY, "cell_footprint_data_silver")
    )

    network_do.df = spark.createDataFrame(input_network, schema=SilverNetworkDataObject.SCHEMA)
    events_do.df = spark.createDataFrame(input_events, schema=SilverEventDataObject.SCHEMA)
    footprint_do.df = spark.createDataFrame(input_footprint, schema=SilverCellFootprintDataObject.SCHEMA)

    network_do.write()
    events_do.write()
    footprint_do.write()
