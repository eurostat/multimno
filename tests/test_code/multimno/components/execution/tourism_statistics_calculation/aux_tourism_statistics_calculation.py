from configparser import ConfigParser
from datetime import date, datetime
from multimno.core.constants.columns import ColNames

from multimno.core.data_objects.silver.silver_tourism_stays_data_object import SilverTourismStaysDataObject
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.data_objects.bronze.bronze_mcc_iso_tz_map import BronzeMccIsoTzMap
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from tests.test_code.fixtures import spark_session as spark


input_tourism_stays_id = "tourism_stays_silver"
input_tourism_trips_id = "tourism_trips_silver"
input_mcc_iso_tz_map_id = "mcc_iso_timezones_data_bronze"

expected_tourism_trips_id = "expected_tourism_trips_silver"
expected_tourism_geozone_aggregations_id = "expected_tourism_geozone_aggregations_silver"
expected_tourism_trip_aggregations_id = "expected_tourism_trip_aggregations_silver"


def get_expected_trips_output_df(spark: SparkSession, expected_trips_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismTripDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_trips_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_trips_data, schema=SilverTourismTripDataObject.SCHEMA)
    return expected_data_df


def get_expected_agg_i_output_df(spark: SparkSession, expected_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismGeographicalZoneNightsSpentAndDeparturesDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(
        expected_data, schema=SilverTourismZoneDeparturesNightsSpentDataObject.SCHEMA
    )
    return expected_data_df


def get_expected_agg_ii_output_df(spark: SparkSession, expected_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismTripAverageDestinationsAndNightsSpentDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(
        expected_data, schema=SilverTourismTripAvgDestinationsNightsSpentDataObject.SCHEMA
    )
    return expected_data_df


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    tourism_stays_data: list[Row],
    tourism_trips_data: list[Row],
    mcc_iso_tz_map_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        tourism_stays_data (list[Row]): list of tourism stays data rows
        tourism_trips_data (list[Row]): list of tourism trips data rows
    """

    ### Write input tourism stays data to test resources dir

    tourism_stays_data_path = config["Paths.Silver"][input_tourism_stays_id]
    input_tourism_stays_do = SilverTourismStaysDataObject(spark, tourism_stays_data_path)
    input_tourism_stays_do.df = spark.createDataFrame(
        tourism_stays_data, schema=SilverTourismStaysDataObject.SCHEMA
    ).orderBy(ColNames.user_id, ColNames.start_timestamp)
    partition_columns = SilverTourismStaysDataObject.PARTITION_COLUMNS
    input_tourism_stays_do.write(partition_columns=partition_columns)

    ### Write input tourism trips data to test resources dir
    tourism_trips_data_path = config["Paths.Silver"][input_tourism_trips_id]
    tourism_trips_do = SilverTourismTripDataObject(spark, tourism_trips_data_path)
    tourism_trips_do.df = spark.createDataFrame(tourism_trips_data, schema=SilverTourismTripDataObject.SCHEMA)
    partition_columns = SilverTourismTripDataObject.PARTITION_COLUMNS
    tourism_trips_do.write(partition_columns=partition_columns)

    ### Write input mcc_iso_tz_map data to test resources dir
    mcc_iso_tz_map_path = config["Paths.Bronze"][input_mcc_iso_tz_map_id]
    mcc_iso_tz_map_do = BronzeMccIsoTzMap(spark, mcc_iso_tz_map_path)
    mcc_iso_tz_map_do.df = spark.createDataFrame(mcc_iso_tz_map_data, schema=BronzeMccIsoTzMap.SCHEMA)
    mcc_iso_tz_map_do.write()


def data_test_0001() -> dict:
    date_format = "%Y-%m-%dT%H:%M:%S"

    input_stays_data = []
    input_trips_data = []
    input_mcc_iso_map_data = []
    output_trips_data = []
    output_aggs_i_data = []
    output_aggs_ii_data = []

    # Add user data
    input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data = add_user_a(
        input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data, date_format
    )

    input_mcc_iso_map_data = get_mcc_iso_tz_map_data()

    return {
        input_tourism_stays_id: input_stays_data,
        input_tourism_trips_id: input_trips_data,
        input_mcc_iso_tz_map_id: input_mcc_iso_map_data,
        expected_tourism_trips_id: output_trips_data,
        expected_tourism_geozone_aggregations_id: output_aggs_i_data,
        expected_tourism_trip_aggregations_id: output_aggs_ii_data,
    }


def data_test_0002() -> dict:
    #
    date_format = "%Y-%m-%dT%H:%M:%S"

    input_stays_data = []
    input_trips_data = []
    input_mcc_iso_map_data = []
    output_trips_data = []
    output_aggs_i_data = []
    output_aggs_ii_data = []

    # Add user data
    input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data = add_user_b(
        input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data, date_format
    )

    input_mcc_iso_map_data = get_mcc_iso_tz_map_data()

    # input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data = add_user_c(input_stays_data, input_trips_data, output_trips_data, output_aggs_i_data, output_aggs_ii_data, date_format)

    return {
        input_tourism_stays_id: input_stays_data,
        input_tourism_trips_id: input_trips_data,
        input_mcc_iso_tz_map_id: input_mcc_iso_map_data,
        expected_tourism_trips_id: output_trips_data,
        expected_tourism_geozone_aggregations_id: output_aggs_i_data,
        expected_tourism_trip_aggregations_id: output_aggs_ii_data,
    }


def get_mcc_iso_tz_map_data() -> list[Row]:
    return [
        Row(
            mcc=214,
            name="Spain",
            iso2="ES",
            iso3="ESP",
            eurostat_code="ES",
            latitude=40.463667,
            longitude=-3.74922,
            timezone="Europe/Madrid",
        ),
        Row(
            mcc=222,
            name="Italy",
            iso2="IT",
            iso3="ITA",
            eurostat_code="IT",
            latitude=41.87194,
            longitude=12.56738,
            timezone="Europe/Rome",
        ),
        Row(
            mcc=270,
            name="Luxembourg",
            iso2="LU",
            iso3="LUX",
            eurostat_code="LU",
            latitude=49.815273,
            longitude=6.129583,
            timezone="Europe/London",
        ),
    ]


def add_user_a(
    input_stays_data,
    input_trips_data,
    output_trips_data,
    output_aggs_i_data,
    output_aggs_ii_data,
    date_format,
) -> tuple[list[Row], list[Row], list[Row], list[Row], list[Row]]:
    """
    Add user A data.
    Ongoing trip with ongoing visit in first month, trip ends in second month.
    """
    user_id_a = "user_a".encode("ascii")
    user_id_modulo = 0
    iso2 = "ES"
    mcc = 214
    mnc = "01"
    plmn = 0
    year = 2023
    month = 1
    h_id_1_1_1 = "z0|z00|z001"
    h_id_1_1_2 = "z0|z00|z002"
    h_id_1_1_3 = "z0|z00|z003"
    h_id_1_2_1 = "z0|z01|z011"
    h_id_1_1 = "z0|z00"
    h_id_1_2 = "z0|z01"
    h_id_1 = "z0"
    h_id_2 = "z1"
    dataset_id = "test_dataset"

    user_trips_data = [  # ongoing trips
        Row(
            user_id=user_id_a,
            trip_id="bbf85750c68114bcaaff7bb72273f08d",
            # time_segments_arr=[(date(2022, 12, 30), date(2022, 12, 30), [1, 2]), (date(2022, 12, 30), None, [3, 4])],
            trio_start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
            time_segment_ids_list=[1, 2, 3, 4],
            # end_timestamp=None,
            is_trip_finished=False,
            year=2022,
            month=12,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
    ]

    user_stays_data = [
        Row(  # Stay in first location (three-zone stay)
            user_id=user_id_a,
            time_segment_id=1,
            start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T15:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1, h_id_1_1_2, h_id_1_1_3],
            zone_weights_list=[0.4, 0.4, 0.2],
            is_overnight=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Same location, three-zone
            user_id=user_id_a,
            time_segment_id=2,
            start_timestamp=datetime.strptime("2022-12-30T16:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T16:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1, h_id_1_1_2, h_id_1_1_3],
            zone_weights_list=[0.4, 0.4, 0.2],
            is_overnight=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Stay in second location
            user_id=user_id_a,
            time_segment_id=3,
            start_timestamp=datetime.strptime("2022-12-30T17:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T17:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_2_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_a,
            time_segment_id=4,
            start_timestamp=datetime.strptime("2022-12-30T18:00:00", date_format),
            end_timestamp=datetime.strptime("2022-12-30T19:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_2_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2022,
            month=12,
            day=30,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Stay in next month
            user_id=user_id_a,
            time_segment_id=5,
            start_timestamp=datetime.strptime("2023-01-01T06:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T08:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_a,
            time_segment_id=6,
            start_timestamp=datetime.strptime("2023-01-01T09:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1, h_id_1_1_2],
            zone_weights_list=[0.7, 0.3],
            is_overnight=True,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Stay in month 2023-02
            user_id=user_id_a,
            time_segment_id=10,
            start_timestamp=datetime.strptime("2023-02-20T09:00:00", date_format),
            end_timestamp=datetime.strptime("2023-02-20T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_3],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=2,
            day=20,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
    ]

    user_output_trips_data = [
        Row(
            user_id=user_id_a,
            trip_id="bbf85750c68114bcaaff7bb72273f08d",
            trip_start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
            time_segment_ids_list=[1, 2, 3, 4],
            is_trip_finished=False,
            year=2022,
            month=12,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_a,
            trip_id="bbf85750c68114bcaaff7bb72273f08d",
            trip_start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
            time_segment_ids_list=[1, 2, 3, 4, 5, 6],
            is_trip_finished=True,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_a,
            trip_id="91776a5ce1e630a209e43e2f197506df",
            trip_start_timestamp=datetime.strptime("2023-02-20T09:00:00", date_format),
            time_segment_ids_list=[10],
            is_trip_finished=True,
            year=2023,
            month=2,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
    ]

    user_output_aggs_i_data = [
        Row(
            time_period="2023-01",
            zone_id="z0",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z0",
            country_of_origin=iso2,
            is_overnight=True,
            nights_spent=1.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z00",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z00",
            country_of_origin=iso2,
            is_overnight=True,
            nights_spent=1.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z01",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z001",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z001",
            country_of_origin=iso2,
            is_overnight=True,
            nights_spent=0.7,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z002",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z002",
            country_of_origin=iso2,
            is_overnight=True,
            nights_spent=0.3,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z003",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z011",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z0",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z00",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z003",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
    ]
    user_output_aggs_ii_data = [
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=1.0,
            year=2023,
            month=1,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=2.0,
            avg_nights_spent_per_destination=0.5,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=4.0,
            avg_nights_spent_per_destination=0.25,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
    ]

    input_stays_data += user_stays_data
    input_trips_data += user_trips_data
    output_trips_data += user_output_trips_data
    output_aggs_i_data += user_output_aggs_i_data
    output_aggs_ii_data += user_output_aggs_ii_data
    return (
        input_stays_data,
        input_trips_data,
        output_trips_data,
        output_aggs_i_data,
        output_aggs_ii_data,
    )


def add_user_b(
    input_stays_data,
    input_trips_data,
    output_trips_data,
    output_aggs_i_data,
    output_aggs_ii_data,
    date_format,
) -> tuple[list[Row], list[Row], list[Row], list[Row], list[Row]]:
    """
    Add user B data.
    User with no ongoing trip. Has new current month trip ending in current month.
    Has new trip starting in current month and ending in the next month.
    """
    user_id_b = "user_b".encode("ascii")
    user_id_modulo = 0
    iso2 = "IT"
    mcc = 222
    mnc = "01"
    plmn = 0
    year = 2023
    month = 1
    h_id_1_1_1 = "z0|z00|z001"
    h_id_1_1_2 = "z0|z00|z002"
    h_id_1_1_3 = "z0|z00|z003"
    h_id_1_1 = "z0|z00"
    h_id_1_2 = "z0|z01"
    h_id_1 = "z0"
    h_id_2 = "z1"
    dataset_id = "test_dataset"

    user_trips_data = [
        # Row(
        #     user_id=user_id_b,
        #     trip_id=1,
        #     time_segments_arr=[
        #         (date(2022,12,30), date(2022,12,30), [1,2]),
        #         (date(2022,12,30), None, [3,4])
        #         ],
        #     start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
        #     end_timestamp=None,
        #     is_trip_finished=False,
        #     year=year,
        #     month=month,
        #     user_id_modulo=user_id_modulo,
        # ),
    ]

    user_stays_data = [
        Row(  # Stay in 2023-01 month ending in the same month
            user_id=user_id_b,
            time_segment_id=99,
            start_timestamp=datetime.strptime("2023-01-01T08:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T09:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_b,
            time_segment_id=100,
            start_timestamp=datetime.strptime("2023-01-01T10:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_2],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # New trip starting at the end of the month, continuing in the next month
            user_id=user_id_b,
            time_segment_id=200,
            start_timestamp=datetime.strptime("2023-01-31T22:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-31T22:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1, h_id_1_1_2],
            zone_weights_list=[0.35, 0.65],
            is_overnight=False,
            year=2023,
            month=1,
            day=31,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Same trip continuing in the next month
            user_id=user_id_b,
            time_segment_id=201,
            start_timestamp=datetime.strptime("2023-02-01T03:00:00", date_format),
            end_timestamp=datetime.strptime("2023-02-01T04:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_2, h_id_1_1_3],
            zone_weights_list=[0.22, 0.78],
            is_overnight=False,
            year=2023,
            month=2,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
    ]

    user_output_trips_data = [
        Row(
            user_id=user_id_b,
            trip_id="b8013cb9f3841dbb3ced846a17a88c31",
            trip_start_timestamp=datetime.strptime("2023-01-01T08:00:00", date_format),
            time_segment_ids_list=[99, 100],
            is_trip_finished=True,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_b,
            trip_id="a8551efa3b60501e6bb77e37fd2bc15f",
            trip_start_timestamp=datetime.strptime("2023-01-31T22:00:00", date_format),
            time_segment_ids_list=[200, 201],
            is_trip_finished=False,
            year=2023,
            month=1,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_b,
            trip_id="a8551efa3b60501e6bb77e37fd2bc15f",
            trip_start_timestamp=datetime.strptime("2023-01-31T22:00:00", date_format),
            time_segment_ids_list=[200, 201],
            is_trip_finished=True,
            year=2023,
            month=2,
            user_id_modulo=0,
            dataset_id="test_dataset",
        ),
    ]

    user_output_aggs_i_data = [
        Row(
            time_period="2023-01",
            zone_id="z0",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z00",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z001",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z002",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            zone_id="z003",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=0.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z0",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z00",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z001",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z002",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            zone_id="z003",
            country_of_origin=iso2,
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=1.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
    ]
    user_output_aggs_ii_data = [
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=1,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=1,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-01",
            country_of_origin=iso2,
            avg_destinations=2.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=1,
            level=3,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=1,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=1.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=2,
            dataset_id="test_dataset",
        ),
        Row(
            time_period="2023-02",
            country_of_origin=iso2,
            avg_destinations=3.0,
            avg_nights_spent_per_destination=0.0,
            year=2023,
            month=2,
            level=3,
            dataset_id="test_dataset",
        ),
    ]

    input_stays_data += user_stays_data
    input_trips_data += user_trips_data
    output_trips_data += user_output_trips_data
    output_aggs_i_data += user_output_aggs_i_data
    output_aggs_ii_data += user_output_aggs_ii_data
    return (
        input_stays_data,
        input_trips_data,
        output_trips_data,
        output_aggs_i_data,
        output_aggs_ii_data,
    )


def add_user_c(
    input_stays_data,
    input_trips_data,
    output_trips_data,
    output_aggs_i_data,
    output_aggs_ii_data,
    date_format,
) -> tuple[list[Row], list[Row], list[Row], list[Row], list[Row]]:
    """
    Add user C data.
    User with no ongoing trip and multiple current month trips.
    """
    user_id_b = "user_c".encode("ascii")
    user_id_modulo = 0
    iso2 = "LU"
    mcc = 270
    mnc = "01"
    plmn = 0
    year = 2023
    month = 1
    h_id_1_1_1 = "z0|z00|z001"
    h_id_1_1_2 = "z0|z00|z002"
    h_id_1_1 = "z0|z00"
    h_id_1_2 = "z0|z01"
    h_id_1 = "z0"
    h_id_2 = "z1"

    user_trips_data = [
        # Row(
        #     user_id=user_id_b,
        #     trip_id=1,
        #     time_segments_arr=[
        #         (date(2022,12,30), date(2022,12,30), [1,2]),
        #         (date(2022,12,30), None, [3,4])
        #         ],
        #     start_timestamp=datetime.strptime("2022-12-30T00:00:00", date_format),
        #     end_timestamp=None,
        #     is_trip_finished=False,
        #     year=year,
        #     month=month,
        #     user_id_modulo=user_id_modulo,
        # ),
    ]

    user_stays_data = [
        Row(  # First trip
            user_id=user_id_b,
            time_segment_id=201,
            start_timestamp=datetime.strptime("2023-01-01T08:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T09:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_b,
            time_segment_id=202,
            start_timestamp=datetime.strptime("2023-01-01T10:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-01T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(  # Start of second trip
            user_id=user_id_b,
            time_segment_id=211,
            start_timestamp=datetime.strptime("2023-01-14T08:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-14T09:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[
                h_id_1_1_1,
            ],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
        Row(
            user_id=user_id_b,
            time_segment_id=212,
            start_timestamp=datetime.strptime("2023-01-14T10:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-14T11:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            zone_ids_list=[h_id_1_1_1],
            zone_weights_list=[1.0],
            is_overnight=False,
            year=2023,
            month=1,
            day=1,
            user_id_modulo=user_id_modulo,
            dataset_id="test_dataset",
        ),
    ]

    input_stays_data += user_stays_data
    input_trips_data += user_trips_data
    return input_stays_data, input_trips_data
