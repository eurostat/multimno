from datetime import datetime, timedelta
from pyspark.sql.types import Row
import calendar as cal
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs


def generate_input_population_grid_data(timestamp: str) -> list[Row]:
    """
    Generate input population grid data.
    """

    t1 = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

    return [
        Row(
            grid_id=15209001896100,
            population=1.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=15209001896300,
            population=5.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=15209001896900,
            population=8.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=15211001896100,
            population=4.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=15211001896600,
            population=2.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
    ]


def generate_input_ue_grid_data(start_date: str, end_date: str) -> list[Row]:
    """
    Generate input population grid data.
    """
    # create end date from start date. Just date not time
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)

    expected_output_data = [
        Row(
            grid_id=15209001896100,
            weighted_device_count=1.0,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15209001896100,
            weighted_device_count=1.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15209001896300,
            weighted_device_count=5.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15209001896300,
            weighted_device_count=5.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15209001896900,
            weighted_device_count=8.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15209001896900,
            weighted_device_count=8.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15211001896100,
            weighted_device_count=4.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15211001896100,
            weighted_device_count=4.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15211001896600,
            weighted_device_count=2.0,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=15211001896600,
            weighted_device_count=2.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]
    return expected_output_data


def generate_zone_to_grid_map_data(date: str) -> list[Row]:
    """
    Generate zone to grid mapping input data.
    """
    timestamp = datetime.strptime(date, "%Y-%m-%d")
    dataset_id = "nuts"
    return [
        Row(
            grid_id=15209001896100,
            zone_id="C01",
            hierarchical_id="A01|B01|C01",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=15209001896300,
            zone_id="C02",
            hierarchical_id="A01|B01|C02",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=15209001896900,
            zone_id="C02",
            hierarchical_id="A01|B01|C02",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=15211001896100,
            zone_id="C03",
            hierarchical_id="A01|B02|C03",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=15211001896600,
            zone_id="C03",
            hierarchical_id="A01|B02|C03",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
    ]


def generate_expected_population_zone_data(timestamp: str) -> list[Row]:
    """
    Generate expected output data by aggregating population over hierarchical zones.
    """
    t1 = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    dataset_id = "nuts"
    year = t1.year
    month = t1.month
    day = t1.day

    return [
        # Level 1 aggregation
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
        # Level 2 aggregations
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
        # Level 3 aggregations
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


def generate_expected_ue_zone_data(start_date: str, end_date: str) -> list[Row]:
    """
    Generate expected output data by aggregating weighted_device_count over hierarchical zones and labels.
    """
    # Parse the start and end dates
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    dataset_id = "nuts"
    season = "all"

    expected_output_data = [
        # Level 1 Aggregation (A01)
        Row(
            zone_id="A01",
            weighted_device_count=3.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=17.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="A01",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        # Level 2 Aggregations
        # B01
        Row(
            zone_id="B01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="B01",
            weighted_device_count=14.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        # B02
        Row(
            zone_id="B02",
            weighted_device_count=2.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=4.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="B02",
            weighted_device_count=6.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        # Level 3 Aggregations
        # C01
        Row(
            zone_id="C01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="C01",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        # C02
        Row(
            zone_id="C02",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="C02",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        # C03
        Row(
            zone_id="C03",
            weighted_device_count=2.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=4.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="C03",
            weighted_device_count=6.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
    ]

    return expected_output_data


def generate_expected_population_1km_grid_data(timestamp: str) -> list[Row]:
    """
    Generate expected output data by aggregating population over 1km grid.
    """
    t1 = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
    dataset_id = ReservedDatasetIDs.INSPIRE_1km
    year = t1.year
    month = t1.month
    day = t1.day

    return [
        Row(
            zone_id="15200001896000",
            population=14.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            zone_id="15210001896000",
            population=6.0,
            timestamp=t1,
            dataset_id=dataset_id,
            level=1,
            year=year,
            month=month,
            day=day,
        ),
    ]


def generate_expected_ue_1km_grid_data(start_date: str, end_date: str) -> list[Row]:
    """
    Generate expected output data by aggregating weighted_device_count over 1km grid and labels.
    """
    # Parse the start and end dates
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    dataset_id = ReservedDatasetIDs.INSPIRE_1km
    season = "all"

    expected_output_data = [
        Row(
            zone_id="15200001896000",
            weighted_device_count=1.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="15200001896000",
            weighted_device_count=14.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="15200001896000",
            weighted_device_count=13.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="15210001896000",
            weighted_device_count=2.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="15210001896000",
            weighted_device_count=6.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
        Row(
            zone_id="15210001896000",
            weighted_device_count=4.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season=season,
        ),
    ]

    return expected_output_data
