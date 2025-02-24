import datetime as dt
from pyspark.sql import Row
from pyspark.sql import SparkSession
from configparser import ConfigParser

from multimno.core.data_objects.silver.silver_present_population_data_object import SilverPresentPopulationDataObject
from multimno.core.data_objects.silver.silver_present_population_zone_data_object import (
    SilverPresentPopulationZoneDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_zones_data_object import (
    SilverAggregatedUsualEnvironmentsZonesDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs


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

    input_data = [
        Row(
            zone_id="A01",
            weighted_device_count=3.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=17.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=14.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=2.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=4.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=6.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=2.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=4.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=6.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return input_data


def generate_expected_estimated_aggregated_ue_zone_data(start_date: str) -> list[Row]:
    """Generate the expected output of the test of present populatio estimation.

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

    return expected_output_data


def generate_input_aggregated_ue_100m_data(start_date: str) -> list[Row]:
    """Generates the test's input usual environment data

    Args:
        start_date (str): date to use for setting the `start_date` and `end_date` fields.

    Returns:
        list[Row]: list of rows that form the input data.
    """
    # Parse the start and end dates
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = start_date_dt + dt.timedelta(days=89)
    season = "all"

    input_data = [
        Row(
            zone_id=11111112222222,
            weighted_device_count=3.0,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111112222222,
            weighted_device_count=17.0,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111112222222,
            weighted_device_count=20.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111113333333,
            weighted_device_count=1.0,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111113333333,
            weighted_device_count=13.0,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111113333333,
            weighted_device_count=14.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111114444444,
            weighted_device_count=2.0,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111114444444,
            weighted_device_count=4.0,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111114444444,
            weighted_device_count=6.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111115555555,
            weighted_device_count=1.0,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111115555555,
            weighted_device_count=1.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111116666666,
            weighted_device_count=13.0,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111116666666,
            weighted_device_count=13.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111117777777,
            weighted_device_count=2.0,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111117777777,
            weighted_device_count=4.0,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id=11111117777777,
            weighted_device_count=6.0,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
    ]

    return input_data


def generate_expected_estimated_aggregated_ue_100m_data(start_date: str) -> list[Row]:
    """Generate the expected output of the test of present populatio estimation.

    Args:
        start_date (str): date to use for setting the `start_date` and `end_date` fields.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    # Parse the start and end dates
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = start_date_dt + dt.timedelta(days=89)
    dataset_id = ReservedDatasetIDs.INSPIRE_100m
    season = "all"

    expected_output_data = [
        Row(
            zone_id="100mN11111E22222",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E22222",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E22222",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E33333",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E33333",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E33333",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E44444",
            weighted_device_count=10.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E44444",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E44444",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E55555",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E55555",
            weighted_device_count=5.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E66666",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E66666",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E77777",
            weighted_device_count=10.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E77777",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date_dt,
            end_date=end_date_dt,
            season=season,
        ),
        Row(
            zone_id="100mN11111E77777",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=1,
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
            population=20.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B01",
            population=14.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="B02",
            population=6.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=2,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C01",
            population=1.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C02",
            population=13.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="C03",
            population=6.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=3,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_expected_estimated_present_population_zones_data(timestamp: str) -> list[Row]:
    """Generate the expected output of the test of present populatio estimation.

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


def generate_input_present_population_100m_data(timestamp: str) -> list[Row]:
    """Generates the test's input present population data

    Args:
        timestamp (str): timestamp to use for setting the `timestamp` fields.

    Returns:
        list[Row]: list of rows that form the input data.
    """
    t1 = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    year = t1.year
    month = t1.month
    day = t1.day

    return [
        Row(
            zone_id=11111112222222,
            population=20.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id=11111113333333,
            population=14.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id=11111114444444,
            population=6.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id=11111115555555,
            population=1.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id=11111116666666,
            population=13.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id=11111117777777,
            population=6.0,
            timestamp=t1,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_expected_estimated_present_population_100m_data(timestamp: str) -> list[Row]:
    """Generate the expected output of the test of present populatio estimation.

    Args:
        timestamp (str): timestamp to use for setting the `timestamp` fields.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    t1 = dt.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    dataset_id = ReservedDatasetIDs.INSPIRE_100m
    year = t1.year
    month = t1.month
    day = t1.day

    # Dedup factor = 0.5, mno->target pop factor = 10.0

    return [
        Row(
            zone_id="100mN11111E22222",
            population=100.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="100mN11111E33333",
            population=70.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="100mN11111E44444",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="100mN11111E55555",
            population=5.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="100mN11111E66666",
            population=65.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="100mN11111E77777",
            population=30.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_input_internal_migration_data() -> list[Row]:
    """Generates the test's input internal migration data

    Returns:
        list[Row]: list of rows that form the input data.
    """
    input_data = [
        {
            "previous_zone": "2",
            "new_zone": "3",
            "migration": 0.2,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
        {
            "previous_zone": "1",
            "new_zone": "3",
            "migration": 0.3,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
        {
            "previous_zone": "2",
            "new_zone": "1",
            "migration": 0.2,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
    ]

    return input_data


def generate_expected_internal_migration_data() -> list[Row]:
    """Generate the expected output of the test of the estimation of internal migration.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    # Dedup factor = 0.5, mno->target pop factor = 10.0

    return [
        {
            "previous_zone": "2",
            "new_zone": "3",
            "migration": 1.0,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
        {
            "previous_zone": "1",
            "new_zone": "3",
            "migration": 1.5,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
        {
            "previous_zone": "2",
            "new_zone": "1",
            "migration": 1.0,
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.date(2023, 1, 1),
            "end_date_previous": dt.date(2023, 6, 30),
            "season_previous": "all",
            "start_date_new": dt.date(2023, 7, 1),
            "end_date_new": dt.date(2023, 12, 31),
            "season_new": "all",
        },
    ]


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    kind: str,
    at_grid_level: bool = False,
    date: str = "2023-01-01",
    timestamp="2023-01-01T00:00:00",
):
    """Setup function for the input data of the test, creating the necessary data and saving it into a temporary
    directory

    Args:
        spark (SparkSession)
        config (ConfigParser)
        kind (str): what test is being performed. Must be either `usual_environment`, `present_population`, or
            `internal_migration`.
        date (str, optional): date value for input test data for usual_environment test. Defaults to "2023-01-01".
        timestamp (str, optional): timestamp value for input test data for present_population test. Defaults to
            "2023-01-01T00:00:00".

    Raises:
        ValueError: Unsupported `kind` parameter value was passed.
    """
    if kind == "present_population":
        if at_grid_level:
            input_rows = generate_input_present_population_100m_data(timestamp)
            input_do = SilverPresentPopulationDataObject(spark, config["Paths.Silver"]["present_population_silver"])
            input_do.df = spark.createDataFrame(input_rows, schema=SilverPresentPopulationDataObject.SCHEMA)
            input_do.write()
        else:
            input_rows = generate_input_present_population_zones_data(timestamp)
            input_do = SilverPresentPopulationZoneDataObject(
                spark, config["Paths.Silver"]["present_population_zone_silver"]
            )
            input_do.df = spark.createDataFrame(input_rows, schema=SilverPresentPopulationZoneDataObject.SCHEMA)
            input_do.write()

    elif kind == "usual_environment":
        if at_grid_level:
            input_rows = generate_input_aggregated_ue_100m_data(date)
            input_do = SilverAggregatedUsualEnvironmentsDataObject(
                spark, config["Paths.Silver"]["aggregated_usual_environments_silver"]
            )
            input_do.df = spark.createDataFrame(input_rows, schema=SilverAggregatedUsualEnvironmentsDataObject.SCHEMA)
            input_do.write()
        else:
            input_rows = generate_input_aggregated_ue_zone_data(date)
            input_do = SilverAggregatedUsualEnvironmentsZonesDataObject(
                spark, config["Paths.Silver"]["aggregated_usual_environments_zone_silver"]
            )
            input_do.df = spark.createDataFrame(
                input_rows, schema=SilverAggregatedUsualEnvironmentsZonesDataObject.SCHEMA
            )
            input_do.write()

    elif kind == "internal_migration":
        if at_grid_level:
            raise NotImplementedError("test input data for InternalMigration's Estimation at 100m grid not implemented")
        input_rows = generate_input_internal_migration_data()
        input_do = SilverInternalMigrationDataObject(spark, config["Paths.Silver"]["internal_migration_silver"])
        input_do.df = spark.createDataFrame(input_rows, schema=SilverInternalMigrationDataObject.SCHEMA)
        input_do.write()
    else:
        raise ValueError(kind)
