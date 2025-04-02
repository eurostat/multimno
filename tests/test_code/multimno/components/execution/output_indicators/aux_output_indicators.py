import datetime as dt
from pyspark.sql import Row
from pyspark.sql import SparkSession
import sedona.sql.st_constructors as STC
from configparser import ConfigParser

from multimno.core.utils import apply_schema_casting
from multimno.core.data_objects.silver.silver_grid_data_object import SilverGridDataObject
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import SilverGeozonesGridMapDataObject
from multimno.core.data_objects.silver.silver_present_population_data_object import SilverPresentPopulationDataObject
from multimno.core.data_objects.silver.silver_aggregated_usual_environments_data_object import (
    SilverAggregatedUsualEnvironmentsDataObject,
)
from multimno.core.data_objects.silver.silver_internal_migration_data_object import SilverInternalMigrationDataObject
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
from multimno.core.data_objects.bronze.bronze_inbound_estimation_factors_data_object import (
    BronzeInboundEstimationFactorsDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)


def generate_input_internal_migration_data() -> list[Row]:
    """Generates the test's input internal migration data

    Returns:
        list[Row]: list of rows that form the input data.
    """
    input_data = [
        {
            "previous_zone": "2",
            "new_zone": "3",
            "migration": 20.4,
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
            "migration": 30.0,
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


def generate_input_present_population_data() -> list[Row]:
    """
    Generate input population grid data.
    """

    t1 = dt.datetime(2023, 1, 3, 12, 34, 56)

    return [
        Row(
            grid_id=(15209 << 16) + 18961,
            population=1.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=(15209 << 16) + 18963,
            population=5.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=(15209 << 16) + 18969,
            population=8.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=(15211 << 16) + 18961,
            population=4.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=(15211 << 16) + 18966,
            population=2.0,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
    ]


def generate_input_usual_environment_data() -> list[Row]:
    """
    Generate input population grid data.
    """
    # create end date from start date. Just date not time
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2023, 6, 30)

    input_data = [
        Row(
            grid_id=(15209 << 16) + 18961,
            weighted_device_count=1.0,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15209 << 16) + 18961,
            weighted_device_count=1.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15209 << 16) + 18963,
            weighted_device_count=5.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15209 << 16) + 18963,
            weighted_device_count=5.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15209 << 16) + 18969,
            weighted_device_count=8.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15209 << 16) + 18969,
            weighted_device_count=8.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15211 << 16) + 18961,
            weighted_device_count=4.0,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15211 << 16) + 18961,
            weighted_device_count=4.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15211 << 16) + 18966,
            weighted_device_count=2.0,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            grid_id=(15211 << 16) + 18966,
            weighted_device_count=2.0,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]
    return input_data


def generate_input_inbound_tourism_departures_data() -> list[Row]:
    time_period = "2023-01"
    year = 2023
    month = 1
    dataset_id = "nuts"

    input_data = [
        Row(
            time_period=time_period,
            zone_id="Z011",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=5.7,
            num_of_departures=4.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z012",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=12.3,
            num_of_departures=10.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z021",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=20.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z01",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=18.0,
            num_of_departures=12.0,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z02",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=20.0,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=18.0,
            num_of_departures=12.0,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=20.0,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z011",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=2.3,
            num_of_departures=2.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z012",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=4.7,
            num_of_departures=5.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z021",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=10.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z01",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=7.0,
            num_of_departures=6.0,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z02",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=10.0,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=7.0,
            num_of_departures=6.0,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=10.0,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
    ]

    return input_data


def generate_input_inbound_tourism_averages_data() -> list[Row]:
    time_period = "2023-01"
    dataset_id = "nuts"
    year = 2023
    month = 1

    input_data = [
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=2.3,
            avg_nights_spent_per_destination=1.2,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=8.7,
            avg_nights_spent_per_destination=0.7,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="FR",
            avg_destinations=1.5,
            avg_nights_spent_per_destination=3.4,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=3.4,
            avg_nights_spent_per_destination=2.8,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
    ]

    return input_data


def generate_input_inbound_estimation_factors() -> list[Row]:
    input_data = [
        Row(iso2="IT", deduplication_factor=0.9, mno_to_target_population_factor=3.0),
        Row(iso2="FR", deduplication_factor=None, mno_to_target_population_factor=4.5),
    ]

    return input_data


def generate_input_outbound_tourism_data() -> list[Row]:
    input_data = [
        Row(
            time_period="2023-01",
            country_of_destination="IT",
            nights_spent=20.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-01",
            country_of_destination="FR",
            nights_spent=105.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-01",
            country_of_destination="EE",
            nights_spent=10.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="IT",
            nights_spent=500.0,
            year=2023,
            month=2,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="FR",
            nights_spent=206.0,
            year=2023,
            month=2,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="EE",
            nights_spent=35.0,
            year=2023,
            month=2,
        ),
    ]

    return input_data


def generate_input_grid_data() -> list[Row]:
    input_data = [
        Row(
            geometry="POINT (1520950 1896150)",
            grid_id=(15209 << 16) + 18961,
            origin=0,
            quadkey="1234567",
        ),
        Row(
            geometry="POINT (1520950 1896350)",
            grid_id=(15209 << 16) + 18963,
            origin=0,
            quadkey="1234567",
        ),
        Row(
            geometry="POINT (1520950 1896950)",
            grid_id=(15209 << 16) + 18969,
            origin=0,
            quadkey="1234567",
        ),
        Row(
            geometry="POINT (1521150 1896150)",
            grid_id=(15211 << 16) + 18961,
            origin=0,
            quadkey="1234567",
        ),
        Row(
            geometry="POINT (1521150 1896650)",
            grid_id=(15211 << 16) + 18966,
            origin=0,
            quadkey="1234567",
        ),
    ]

    return input_data


def generate_zone_to_grid_map_data() -> list[Row]:
    """
    Generate zone to grid mapping input data.
    """
    timestamp = dt.datetime(2023, 1, 1)
    dataset_id = "nuts"
    return [
        Row(
            grid_id=(15209 << 16) + 18961,
            zone_id="C01",
            hierarchical_id="A01|B01|C01",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=(15209 << 16) + 18963,
            zone_id="C02",
            hierarchical_id="A01|B01|C02",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=(15209 << 16) + 18969,
            zone_id="C02",
            hierarchical_id="A01|B01|C02",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=(15211 << 16) + 18961,
            zone_id="C03",
            hierarchical_id="A01|B02|C03",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
        Row(
            grid_id=(15211 << 16) + 18966,
            zone_id="C03",
            hierarchical_id="A01|B02|C03",
            dataset_id=dataset_id,
            year=timestamp.year,
            month=timestamp.month,
            day=timestamp.day,
        ),
    ]


def generate_expected_deleted_outbound_tourism_data() -> list[Row]:
    expected_data = [
        Row(
            time_period="2023-01",
            country_of_destination="IT",
            nights_spent=100.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-01",
            country_of_destination="FR",
            nights_spent=525.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-01",
            country_of_destination="EE",
            nights_spent=50.0,
            year=2023,
            month=1,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="IT",
            nights_spent=2500.0,
            year=2023,
            month=2,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="FR",
            nights_spent=1030.0,
            year=2023,
            month=2,
        ),
        Row(
            time_period="2023-02",
            country_of_destination="EE",
            nights_spent=175.0,
            year=2023,
            month=2,
        ),
    ]

    return expected_data


def generate_expected_inbound_tourism_averages_data() -> list[Row]:
    time_period = "2023-01"
    dataset_id = "nuts"
    year = 2023
    month = 1

    expected_data = [
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=2.3,
            avg_nights_spent_per_destination=1.2,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=8.7,
            avg_nights_spent_per_destination=0.7,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="FR",
            avg_destinations=1.5,
            avg_nights_spent_per_destination=3.4,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            country_of_origin="IT",
            avg_destinations=3.4,
            avg_nights_spent_per_destination=2.8,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
    ]

    return expected_data


def generate_expected_obfuscated_inbound_tourism_departures_data() -> list[Row]:
    time_period = "2023-01"
    year = 2023
    month = 1
    dataset_id = "nuts"

    expected_data = [
        Row(
            time_period=time_period,
            zone_id="Z011",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=15.39,
            num_of_departures=-1.0,
            # num_of_departures=10.8,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z012",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=33.21,
            num_of_departures=27.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z021",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=54.0,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z01",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=48.6,
            num_of_departures=32.4,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z02",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=54.0,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="IT",
            is_overnight=True,
            nights_spent=48.6,
            num_of_departures=32.4,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="IT",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=54.0,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z011",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=9.8325,
            num_of_departures=-1.0,
            # num_of_departures=8.55,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z012",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=20.0925,
            num_of_departures=21.375,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z021",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=42.75,
            year=year,
            month=month,
            level=3,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z01",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=29.925,
            num_of_departures=25.65,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z02",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=42.75,
            year=year,
            month=month,
            level=2,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="FR",
            is_overnight=True,
            nights_spent=29.925,
            num_of_departures=25.65,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
        Row(
            time_period=time_period,
            zone_id="Z0",
            country_of_origin="FR",
            is_overnight=False,
            nights_spent=0.0,
            num_of_departures=42.75,
            year=year,
            month=month,
            level=1,
            dataset_id=dataset_id,
        ),
    ]

    return expected_data


def generate_expected_deleted_usual_environment_100m_data() -> list[Row]:
    dataset_id = "INSPIRE_100m"
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2023, 6, 30)

    expected_output_data = [
        # Row(
        #     grid_id="100mN15209E18961",
        #     weighted_device_count=-1.0,
        #     dataset_id=dataset_id,
        #     level=1,
        #     label="home",
        #     start_date=start_date,
        #     end_date=end_date,
        #     season="all",
        # ),
        # Row(
        #     grid_id="100mN15209E18961",
        #     weighted_device_count=-1.0,
        #     dataset_id=dataset_id,
        #     level=1,
        #     label="ue",
        #     start_date=start_date,
        #     end_date=end_date,
        #     season="all",
        # ),
        Row(
            zone_id="100mN15209E18963",
            weighted_device_count=25.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="100mN15209E18963",
            weighted_device_count=25.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="100mN15209E18969",
            weighted_device_count=40.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="100mN15209E18969",
            weighted_device_count=40.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="100mN15211E18961",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="100mN15211E18961",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        # Row(
        #     zone_id="100mN15211E18966",
        #     weighted_device_count=-1.0,
        #     dataset_id=dataset_id,
        #     level=1,
        #     label="home",
        #     start_date=start_date,
        #     end_date=end_date,
        #     season="all",
        # ),
        # Row(
        #     zone_id="100mN15211E18966",
        #     weighted_device_count=-1.0,
        #     dataset_id=dataset_id,
        #     level=1,
        #     label="ue",
        #     start_date=start_date,
        #     end_date=end_date,
        #     season="all",
        # ),
    ]
    return expected_output_data


def generate_expected_obfuscated_usual_environment_1km_data() -> list[Row]:
    dataset_id = "INSPIRE_1km"
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2023, 6, 30)

    expected_output_data = [
        Row(
            zone_id="1kmN1520E1896",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="1kmN1521E1896",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="1kmN1520E1896",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="1kmN1521E1896",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="1kmN1520E1896",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="1kmN1521E1896",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]

    return expected_output_data


def generate_expected_obfuscated_usual_environment_zone_data() -> list[Row]:
    dataset_id = "nuts"
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2023, 6, 30)

    expected_output_data = [
        Row(
            zone_id="C01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C03",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=3,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C03",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C03",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B01",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B01",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B01",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B02",
            weighted_device_count=-1.0,
            dataset_id=dataset_id,
            level=2,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B02",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B02",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]

    return expected_output_data


def generate_expected_deleted_usual_environment_zone_data() -> list[Row]:
    dataset_id = "nuts"
    start_date = dt.date(2023, 1, 1)
    end_date = dt.date(2023, 6, 30)

    expected_output_data = [
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C02",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C03",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=3,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="C03",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=3,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B01",
            weighted_device_count=70.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B01",
            weighted_device_count=65.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B02",
            weighted_device_count=30.0,
            dataset_id=dataset_id,
            level=2,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="B02",
            weighted_device_count=20.0,
            dataset_id=dataset_id,
            level=2,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=15.0,
            dataset_id=dataset_id,
            level=1,
            label="home",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=100.0,
            dataset_id=dataset_id,
            level=1,
            label="ue",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            zone_id="A01",
            weighted_device_count=85.0,
            dataset_id=dataset_id,
            level=1,
            label="work",
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]

    return expected_output_data


def generate_expected_obfuscated_present_population_zone_data() -> list[Row]:
    """ """
    t1 = dt.datetime(2023, 1, 3, 12, 34, 56)
    dataset_id = "nuts"
    year = t1.year
    month = t1.month
    day = t1.day

    return [
        # Level 1 aggregation
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
        # Level 2 aggregations
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


def generate_expected_deleted_present_population_zone_data() -> list[Row]:
    """ """
    t1 = dt.datetime(2023, 1, 3, 12, 34, 56)
    dataset_id = "nuts"
    year = t1.year
    month = t1.month
    day = t1.day

    return [
        # Level 1 aggregation
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
        # Level 2 aggregations
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


def generate_expected_obfuscated_present_population_100m_data() -> list[Row]:

    t1 = dt.datetime(2023, 1, 3, 12, 34, 56)

    return [
        Row(
            zone_id="100mN15209E18961",
            population=-1.0,
            timestamp=t1,
            dataset_id="INSPIRE_100m",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            zone_id="100mN15209E18963",
            population=25.0,
            timestamp=t1,
            dataset_id="INSPIRE_100m",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            zone_id="100mN15209E18969",
            population=40.0,
            timestamp=t1,
            dataset_id="INSPIRE_100m",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            zone_id="100mN15211E18961",
            population=20.0,
            timestamp=t1,
            dataset_id="INSPIRE_100m",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            zone_id="100mN15211E18966",
            population=-1.0,
            timestamp=t1,
            dataset_id="INSPIRE_100m",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
    ]


def generate_expected_deleted_present_population_1km_data() -> list[Row]:

    t1 = dt.datetime(2023, 1, 3, 12, 34, 56)

    return [
        Row(
            zone_id="1kmN1520E1896",
            population=70.0,
            timestamp=t1,
            dataset_id="INSPIRE_1km",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            zone_id="1kmN1521E1896",
            population=30.0,
            timestamp=t1,
            dataset_id="INSPIRE_1km",
            level=1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
    ]


def generate_expected_obfuscated_internal_migration_data() -> list[Row]:
    """Generate the expected output of the test of the output indicators of internal migration.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    # Dedup factor = 0.5, mno->target pop factor = 10.0, k=15, obfuscate
    expected_data = [
        {
            "previous_zone": "2",
            "new_zone": "3",
            "migration": 102.0,
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
            "migration": 150.0,
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
            "migration": -1.0,
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

    return expected_data


def generate_expected_deleted_internal_migration_data() -> list[Row]:
    """Generate the expected output of the test of the output indicators of internal migration.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    # Dedup factor = 0.5, mno->target pop factor = 10.0, k=15, delete
    expected_data = [
        {
            "previous_zone": "2",
            "new_zone": "3",
            "migration": 102.0,
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
            "migration": 150.0,
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

    return expected_data


def set_input_data(spark: SparkSession, config: ConfigParser, use_case: str):
    if use_case == "InternalMigration":
        input_rows = generate_input_internal_migration_data()
        input_do = SilverInternalMigrationDataObject(spark, config["Paths.Silver"]["internal_migration_silver"])
        input_do.df = spark.createDataFrame(input_rows, schema=SilverInternalMigrationDataObject.SCHEMA)
        input_do.write()
    elif use_case == "PresentPopulationEstimation":
        input_rows = generate_input_present_population_data()
        input_do = SilverPresentPopulationDataObject(spark, config["Paths.Silver"]["present_population_silver"])
        input_do.df = spark.createDataFrame(input_rows, schema=SilverPresentPopulationDataObject.SCHEMA)

        zonegrid_rows = generate_zone_to_grid_map_data()
        zonegrid_do = SilverGeozonesGridMapDataObject(spark, config["Paths.Silver"]["geozones_grid_map_data_silver"])
        zonegrid_do.df = spark.createDataFrame(zonegrid_rows, schema=SilverGeozonesGridMapDataObject.SCHEMA)

        input_grid = generate_input_grid_data()
        input_grid = spark.createDataFrame(input_grid).withColumn("geometry", STC.ST_GeomFromEWKT("geometry"))
        input_grid = apply_schema_casting(input_grid, schema=SilverGridDataObject.SCHEMA)
        grid_do = SilverGridDataObject(spark, config["Paths.Silver"]["grid_data_silver"])
        grid_do.df = input_grid

        input_do.write()
        zonegrid_do.write()
        grid_do.write()
    elif use_case == "UsualEnvironmentAggregation":
        input_rows = generate_input_usual_environment_data()
        input_do = SilverAggregatedUsualEnvironmentsDataObject(
            spark, config["Paths.Silver"]["aggregated_usual_environments_silver"]
        )
        input_do.df = spark.createDataFrame(input_rows, schema=SilverAggregatedUsualEnvironmentsDataObject.SCHEMA)

        zonegrid_rows = generate_zone_to_grid_map_data()
        zonegrid_do = SilverGeozonesGridMapDataObject(spark, config["Paths.Silver"]["geozones_grid_map_data_silver"])
        zonegrid_do.df = spark.createDataFrame(zonegrid_rows, schema=SilverGeozonesGridMapDataObject.SCHEMA)

        input_grid = generate_input_grid_data()
        input_grid = spark.createDataFrame(input_grid).withColumn("geometry", STC.ST_GeomFromEWKT("geometry"))
        input_grid = apply_schema_casting(input_grid, schema=SilverGridDataObject.SCHEMA)
        grid_do = SilverGridDataObject(spark, config["Paths.Silver"]["grid_data_silver"])
        grid_do.df = input_grid

        input_do.write()
        zonegrid_do.write()
        grid_do.write()
    elif use_case == "TourismStatisticsCalculation":
        departure_rows = generate_input_inbound_tourism_departures_data()
        average_rows = generate_input_inbound_tourism_averages_data()
        input_factors = generate_input_inbound_estimation_factors()

        departures_do = SilverTourismZoneDeparturesNightsSpentDataObject(
            spark, config["Paths.Silver"]["tourism_geozone_aggregations_silver"]
        )
        departures_do.df = spark.createDataFrame(
            departure_rows, schema=SilverTourismZoneDeparturesNightsSpentDataObject.SCHEMA
        )

        average_do = SilverTourismTripAvgDestinationsNightsSpentDataObject(
            spark, config["Paths.Silver"]["tourism_trip_aggregations_silver"]
        )
        average_do.df = spark.createDataFrame(
            average_rows, schema=SilverTourismTripAvgDestinationsNightsSpentDataObject.SCHEMA
        )

        factors_do = BronzeInboundEstimationFactorsDataObject(
            spark, config["Paths.Bronze"]["inbound_estimation_factors_bronze"]
        )
        factors_do.df = spark.createDataFrame(input_factors, schema=BronzeInboundEstimationFactorsDataObject.SCHEMA)

        departures_do.write()
        average_do.write()
        factors_do.write()
    elif use_case == "TourismOutboundStatisticsCalculation":
        input_rows = generate_input_outbound_tourism_data()

        input_do = SilverTourismOutboundNightsSpentDataObject(
            spark, config["Paths.Silver"]["tourism_outbound_aggregations_silver"]
        )

        input_do.df = spark.createDataFrame(input_rows, schema=SilverTourismOutboundNightsSpentDataObject.SCHEMA)
        input_do.write()
