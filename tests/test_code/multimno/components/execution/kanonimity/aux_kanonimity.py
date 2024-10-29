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


def generate_input_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generate the input data of the test of present populatio estimation.

    Args:
        start_date (str): date to use for setting the `start_date` and `end_date` fields.

    Returns:
        list[Row]: list of rows that form the input output.
    """
    # Parse the start and end dates
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = start_date_dt + dt.timedelta(days=89)
    dataset_id = "nuts"
    season = "all"

    input_data = [
        Row(
            zone_id="A01",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=10.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=10.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return input_data


def generate_expected_obfuscated_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generate the expected output of the test of present population estimation, with obfuscation and k=15.

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
            zone_id="A01",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return expected_output_data


def generate_expected_deleted_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generate the expected output of the test of present population estimation, with deletion and k=15.

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
            zone_id="A01",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=3,
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

    return [
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
            population=70.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        # Level 3 aggregations
        Row(
            zone_id="C01",
            population=5.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=65.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_expected_obfuscated_present_population_zones_data(timestamp: str) -> list[Row]:
    """Generate the expected output of the test of present population estimation, with obfuscation and k=15.

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

    # Dedup factor = 0.5, mno->target pop factor = 10.0

    return [
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
            population=70.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        # Level 3 aggregations
        Row(
            zone_id="C01",
            population=-1.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=65.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_expected_deleted_present_population_zones_data(timestamp: str) -> list[Row]:
    """Generate the expected output of the test of present population estimation, with deletion and k=15.

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

    # Dedup factor = 0.5, mno->target pop factor = 10.0

    return [
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
            population=70.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=65.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]


def set_input_data(
    spark: SparkSession, config: ConfigParser, kind: str, date: str = "2023-01-01", timestamp="2023-01-01T00:00:00"
):
    """Setup function for the input data of the test, creating the necessary data and saving it into a temporary
    directory

    Args:
        spark (SparkSession)
        config (ConfigParser)
        kind (str): what test is being performed. Must be either `usual_environment` or `present_population`.
        date (str, optional): date value for input test data for usual_environment test. Defaults to "2023-01-01".
        timestamp (str, optional): timestamp value for input test data for present_population test. Defaults to
            "2023-01-01T00:00:00".

    Raises:
        ValueError: Unsupported `kind` parameter value was passed.
    """
    if kind == "present_population":
        input_rows = generate_input_present_population_zones_data(timestamp)
        input_do = SilverPresentPopulationZoneDataObject(
            spark, config["Paths.Silver"]["estimated_present_population_zone_silver"]
        )
        input_do.df = spark.createDataFrame(input_rows, schema=SilverPresentPopulationZoneDataObject.SCHEMA)
        input_do.write()
    elif kind == "usual_environment":
        input_rows = generate_input_aggregated_ue_zone_data(date)
        input_do = SilverAggregatedUsualEnvironmentsZonesDataObject(
            spark, config["Paths.Silver"]["estimated_aggregated_usual_environments_zone_silver"]
        )
        input_do.df = spark.createDataFrame(input_rows, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA)
        input_do.write()
    else:
        raise ValueError(kind)
