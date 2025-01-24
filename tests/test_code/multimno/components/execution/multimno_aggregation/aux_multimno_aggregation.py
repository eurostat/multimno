import datetime as dt
from pyspark.sql import Row
from pyspark.sql import SparkSession
from configparser import ConfigParser

from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject


def generate_input_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generates the test's input usual environment data

    Args:
        start_date (str): date to use for setting the `start_date` and `end_date` fields.

    Returns:
        list[Row]: list of rows that form the input data.
    """
    # Parse the start and end dates
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = start_date_dt + dt.timedelta(days=89)
    dataset_id = "nuts"
    season = "all"

    input_data_mno_1 = [
        Row(
            zone_id="B01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=200.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=200.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=200.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    input_data_mno_2 = [
        Row(
            zone_id="B01",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=50.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return [input_data_mno_1, input_data_mno_2]


def generate_expected_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generate the expected output of the test of aggregated UE MNO aggregation.

    Args:
        start_date (str): date to use for setting the `start_date` and `end_date` fields.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    # Parse the start and end dates
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = start_date_dt + dt.timedelta(days=89)
    dataset_id = "nuts"
    season = "all"

    expected_output_data = [
        Row(
            zone_id="B01",
            weighted_device_count=90.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=170.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=170.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=90.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=90.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=170.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return expected_output_data


def generate_input_present_population_zones_data(timestamp: str) -> list[Row]:
    """Generates the test's input present population data

    Args:
        timestamp (str): timestamp to use for setting the `timestamp` fields.

    Returns:
        list[Row]: list of rows that form the input data.
    """
    t1 = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    dataset_id = "nuts"
    year = t1.year
    month = t1.month
    day = t1.day

    input_data_mno_1 = [
        Row(
            zone_id="A01",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B01",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C01",
            population=200.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]

    input_data_mno_2 = [
        Row(
            zone_id="A01",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B01",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C01",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=50.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]

    return [input_data_mno_1, input_data_mno_2]


def generate_expected_present_population_zones_data(timestamp: str) -> list[Row]:
    """Generate the expected output of the test of present population MNO aggregation.

    Args:
        timestamp (str): timestamp to use for setting the `timestamp` fields.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    t1 = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    dataset_id = "nuts"
    year = t1.year
    month = t1.month
    day = t1.day

    expected_output_data = [
        Row(
            zone_id="A01",
            population=90.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B01",
            population=90.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=90.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C01",
            population=170.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=90.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=90.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]

    return expected_output_data


def generate_input_internal_migration_data() -> list[Row]:
    """Generates the test's input internal migration data

    Returns:
        list[Row]: list of rows that form the input data.
    """

    input_data_mno_1 = [
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "3",
                "migration": 20.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
        Row(
            **{
                "previous_zone": "1",
                "new_zone": "3",
                "migration": -1.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "1",
                "migration": 10.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
    ]

    input_data_mno_2 = [
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "3",
                "migration": 50.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
        Row(
            **{
                "previous_zone": "1",
                "new_zone": "3",
                "migration": -1.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "1",
                "migration": 20.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
    ]
    return [input_data_mno_1, input_data_mno_2]


def generate_expected_internal_migration_zones_data() -> list[Row]:
    """Generate the expected output of the test of internal migration MNO aggregation.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    expected_output_data = [
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "3",
                "migration": 26.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
        Row(
            **{
                "previous_zone": "2",
                "new_zone": "1",
                "migration": 12.0,
                "dataset_id": "nuts",
                "level": 1,
                "start_date_previous": dt.date(2023, 1, 1),
                "end_date_previous": dt.date(2023, 6, 30),
                "season_previous": "all",
                "start_date_new": dt.date(2023, 7, 1),
                "end_date_new": dt.date(2023, 12, 31),
                "season_new": "all",
            }
        ),
    ]

    return expected_output_data


def set_input_data(
    spark: SparkSession, config: ConfigParser, kind: str, date: str = "2023-01-01", timestamp="2023-01-01T00:00:00"
):
    """Setup function for the input data of the test, creating the necessary data and saving it into a temporary
    directory

    Args:
        spark (SparkSession)
        config (ConfigParser)
        kind (str): what test is being performed. Must be either `usual_environment`, `present_population`, or
            `internal_migration`..
        date (str, optional): date value for input test data for usual_environment test. Defaults to "2023-01-01".
        timestamp (str, optional): timestamp value for input test data for present_population test. Defaults to
            "2023-01-01T00:00:00".

    Raises:
        ValueError: Unsupported `kind` parameter value was passed.
    """
    if kind == "present_population":
        input_rows_mno_1, input_rows_mno_2 = generate_input_present_population_zones_data(timestamp)
        input_do_1 = SilverPresentPopulationZoneDataObject(
            spark, config["Paths.Gold"]["single_mno_1_present_population_zone_gold"]
        )

        input_do_2 = SilverPresentPopulationZoneDataObject(
            spark, config["Paths.Gold"]["single_mno_2_present_population_zone_gold"]
        )
        input_do_1.df = spark.createDataFrame(input_rows_mno_1, schema=SilverPresentPopulationZoneDataObject.SCHEMA)
        input_do_2.df = spark.createDataFrame(input_rows_mno_2, schema=SilverPresentPopulationZoneDataObject.SCHEMA)

        input_do_1.write()
        input_do_2.write()

    elif kind == "usual_environment":
        input_rows_mno_1, input_rows_mno_2 = generate_input_aggregated_ue_zone_data(date)
        input_do_1 = SilverAggregatedUsualEnvironmentsZonesDataObject(
            spark, config["Paths.Gold"]["single_mno_1_usual_environment_zone_gold"]
        )

        input_do_2 = SilverAggregatedUsualEnvironmentsZonesDataObject(
            spark, config["Paths.Gold"]["single_mno_2_usual_environment_zone_gold"]
        )
        input_do_1.df = spark.createDataFrame(
            input_rows_mno_1, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
        )
        input_do_2.df = spark.createDataFrame(
            input_rows_mno_2, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
        )

        input_do_1.write()
        input_do_2.write()
    elif kind == "internal_migration":
        input_rows_mno_1, input_rows_mno_2 = generate_input_internal_migration_data()
        input_do_1 = SilverInternalMigrationDataObject(
            spark, config["Paths.Gold"]["single_mno_1_internal_migration_gold"]
        )

        input_do_2 = SilverInternalMigrationDataObject(
            spark, config["Paths.Gold"]["single_mno_2_internal_migration_gold"]
        )
        input_do_1.df = spark.createDataFrame(input_rows_mno_1, schema=SilverInternalMigrationDataObject.SCHEMA)
        input_do_2.df = spark.createDataFrame(input_rows_mno_2, schema=SilverInternalMigrationDataObject.SCHEMA)

        input_do_1.write()
        input_do_2.write()
    else:
        raise ValueError(kind)
