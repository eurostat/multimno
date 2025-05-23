import pytest
from configparser import ConfigParser
from datetime import datetime, timedelta, date
from multimno.core.constants.columns import ColNames
from multimno.core.constants.domain_names import Domains
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
import pyspark.sql.functions as F

from multimno.core.utils import apply_schema_casting
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_grid_data_object import (
    SilverGridDataObject,
)
from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]

input_events_id = "input_events"
input_cell_connection_probability_id = "input_cell_connection_probability"
input_grid_id = "input_grid"
input_zone_to_grid_map_id = "input_zone_to_grid_map"
expected_present_population_id = "expected_present_population"


def get_expected_output_df(spark: SparkSession, expected_population_data: list[Row], schema) -> DataFrame:
    """
    Function to turn provided expected result data in list form into Spark DataFrame.
    Provided schema has to match the expected data object's schema.

    Args:
        spark (SparkSession): Spark session
        expected_population_data (list[Row]): list of Rows matching the schema
        schema: Schema of data object

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_population_data, schema=schema)
    return expected_data_df


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    event_data: list[Row],
    grid_data: list[Row],
    cell_connection_prob_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        event_data (list[Row]): list of event data rows
        grid_data (list[Row]): list of grid data rows
        cell_connection_prob_data (list[Row]): list of cell connection probability data rows
        zone_to_grid_map_data (list[Row]): list of zone to grid mappings. Only used if aggregation level is "zone"
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]

    ### Write input event data to test resources dir
    event_data_path = config["Paths.Silver"]["event_data_silver_flagged"]
    input_events_do = SilverEventFlaggedDataObject(spark, event_data_path)
    input_events_do.df = spark.createDataFrame(event_data, schema=SilverEventFlaggedDataObject.SCHEMA).orderBy(
        ColNames.user_id, ColNames.timestamp
    )
    input_events_do.write(partition_columns=partition_columns)

    ### Write input grid data to test resources dir
    # Have to parse geometry string to geometry type after df creation, so we can't use the DO schema right away
    grid_data_path = config["Paths.Silver"]["grid_data_silver"]

    input_grid_do = SilverGridDataObject(spark, grid_data_path)
    grid_df = spark.createDataFrame(grid_data).withColumn(
        ColNames.geometry, F.expr(f"ST_GeomFromWKT({ColNames.geometry})")
    )
    input_grid_do.df = apply_schema_casting(grid_df, SilverGridDataObject.SCHEMA)

    input_grid_do.write(partition_columns=[])

    ### Write input cell connection probability data to test resources dir
    cell_connection_prob_data_path = config["Paths.Silver"]["cell_connection_probabilities_data_silver"]
    input_cell_connection_prob_do = SilverCellConnectionProbabilitiesDataObject(spark, cell_connection_prob_data_path)
    input_cell_connection_prob_do.df = spark.createDataFrame(
        cell_connection_prob_data,
        schema=SilverCellConnectionProbabilitiesDataObject.SCHEMA,
    )
    input_cell_connection_prob_do.write(partition_columns=partition_columns)


def data_test_grid_0001() -> dict:
    """
    Data generation of grid test 0001
    """
    validity_period_start = "2022-01-01"
    validity_period_end = "2025-01-01"

    # Generate grid data. Physical parameters are irrelevant in this component as spatial operations are not done here.
    input_grid_data = []
    input_grid_data += generate_grid_data_0001()
    # Generate cell connection probability data.

    input_cell_connection_probability_data = []
    input_cell_connection_probability_data += generate_cell_connection_probabilities_data_0001(
        [date(2022, 12, 31), date(2023, 1, 1)], validity_period_start, validity_period_end
    )
    # Generate event data.
    input_event_data = []
    input_event_data += generate_event_data_0001()

    # Expected output: population per grid.
    expected_output_data = generate_expected_results_data_0001()
    return {
        input_grid_id: input_grid_data,
        input_cell_connection_probability_id: input_cell_connection_probability_data,
        input_events_id: input_event_data,
        expected_present_population_id: expected_output_data,
    }


def generate_expected_results_data_0001():
    """
    Generate expected results. 0001
    """
    timestamp_format = "%Y-%m-%dT%H:%M:%S"
    t1 = datetime.strptime("2023-01-01T00:00:00", timestamp_format)
    expected_output_data = [
        Row(
            grid_id=1,
            population=0.05780401453375816,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=2,
            population=1.0642139911651611,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=3,
            population=0.48203596472740173,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=4,
            population=0.16694311797618866,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
        Row(
            grid_id=5,
            population=0.2290029227733612,
            timestamp=t1,
            year=t1.year,
            month=t1.month,
            day=t1.day,
        ),
    ]
    return expected_output_data


def generate_event_data_0001():
    # Generate one collection of testing event data. 0001
    timestamp_format = "%Y-%m-%dT%H:%M:%S"
    t1 = datetime.strptime("2023-01-01T00:00:00", timestamp_format)
    t2 = datetime.strptime("2023-01-02T23:58:00", timestamp_format)
    input_event_data = []
    input_event_data += generate_event_data_multiple_events_in_window(
        user_id="multiple_in_window",
        timestamp=t1,
    )
    input_event_data += generate_event_data_events_outside_window(
        user_id="outside_window",
        timestamp=t1,
    )
    input_event_data += generate_event_data_events_in_previous_date(
        user_id="events_in_prev_date",
        timestamp=t1,
    )
    input_event_data += generate_event_data_events_in_next_date(
        user_id="events_in_next_date",
        timestamp=t2,
    )
    return input_event_data


def generate_grid_data_0001():
    """
    Generate one collection of testing grid data. 0001
    10 grid ids with ids 1 to 10.
    """
    grid_data = []
    for i in range(1, 11):
        grid_data.append(
            Row(
                geometry="POINT(0.0 0.0)",
                grid_id=i,
                origin=0,
                quadkey="1234567",
            )
        )
    return grid_data


def generate_cell_connection_probabilities_data_0001(dates, validity_period_start, validity_period_end):
    """
    Generate one collection of testing cell connection probabilities data. 0001
    """

    out = []
    for date in dates:
        year = date.year
        month = date.month
        day = date.day
        out.extend(
            [
                Row(
                    cell_id=1,
                    grid_id=1,
                    cell_connection_probability=0.3,
                    posterior_probability=0.3,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=4,
                    grid_id=1,
                    cell_connection_probability=0.7,
                    posterior_probability=0.7,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=1,
                    grid_id=2,
                    cell_connection_probability=0.6,
                    posterior_probability=0.6,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=2,
                    grid_id=2,
                    cell_connection_probability=0.4,
                    posterior_probability=0.4,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=1,
                    grid_id=3,
                    cell_connection_probability=0.25,
                    posterior_probability=0.25,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=2,
                    grid_id=3,
                    cell_connection_probability=0.75,
                    posterior_probability=0.75,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=2,
                    grid_id=4,
                    cell_connection_probability=0.9,
                    posterior_probability=0.9,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=3,
                    grid_id=4,
                    cell_connection_probability=0.1,
                    posterior_probability=0.1,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=2,
                    grid_id=5,
                    cell_connection_probability=0.5,
                    posterior_probability=0.5,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=2,
                    grid_id=5,
                    cell_connection_probability=0.5,
                    posterior_probability=0.5,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=3,
                    grid_id=5,
                    cell_connection_probability=0.5,
                    posterior_probability=0.5,
                    year=year,
                    month=month,
                    day=day,
                ),
                Row(
                    cell_id=4,
                    grid_id=6,
                    cell_connection_probability=1.0,
                    posterior_probability=1.0,
                    year=year,
                    month=month,
                    day=day,
                ),
            ]
        )
    return out


def generate_event_data_multiple_events_in_window(user_id: str, timestamp: datetime.timestamp):
    """
    Multiple events of one user near a provided timestamp. The nearest event is tied between two events.
    Exactly one shall be chosen as nearest event, preferrring the earlier event if tied.
    Expected result: cell id 2 at (timestamp - timedelta(seconds=60)) is included in aggregated results
    """
    return [
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp - timedelta(seconds=600),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="1",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp - timedelta(seconds=600)).year,
            month=(timestamp - timedelta(seconds=600)).month,
            day=(timestamp - timedelta(seconds=600)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp - timedelta(seconds=60),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="2",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp - timedelta(seconds=60)).year,
            month=(timestamp - timedelta(seconds=60)).month,
            day=(timestamp - timedelta(seconds=60)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp + timedelta(seconds=60),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="3",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp + timedelta(seconds=60)).year,
            month=(timestamp + timedelta(seconds=60)).month,
            day=(timestamp + timedelta(seconds=60)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp + timedelta(seconds=600),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="4",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp + timedelta(seconds=600)).year,
            month=(timestamp + timedelta(seconds=600)).month,
            day=(timestamp + timedelta(seconds=600)).day,
            user_id_modulo=1,
        ),
    ]


def generate_event_data_events_outside_window(user_id: str, timestamp: datetime.timestamp):
    """
    Valid events, but outside any time point windows.
    Expected result: nothing from here is included in aggregated results for this time point
    """
    offset_days = 60
    return [
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp - timedelta(days=offset_days),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="1",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp - timedelta(days=offset_days)).year,
            month=(timestamp - timedelta(days=offset_days)).month,
            day=(timestamp - timedelta(days=offset_days)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp + timedelta(days=offset_days),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="2",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp + timedelta(days=offset_days)).year,
            month=(timestamp + timedelta(days=offset_days)).month,
            day=(timestamp + timedelta(days=offset_days)).day,
            user_id_modulo=1,
        ),
    ]


def generate_event_data_events_in_previous_date(user_id: str, timestamp: datetime.timestamp):
    """
    Valid events in time point window, including an event from the previous date which is the nearest event.
    Assumes a timestamp very close to the previous date is provided as input.
    Expected result: event with cell id 1 at (timestamp - timedelta(seconds=600)) is included in aggregated results
    """
    return [
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp - timedelta(seconds=600),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="1",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp - timedelta(seconds=600)).year,
            month=(timestamp - timedelta(seconds=600)).month,
            day=(timestamp - timedelta(seconds=600)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp + timedelta(seconds=800),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="2",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp + timedelta(seconds=800)).year,
            month=(timestamp + timedelta(seconds=800)).month,
            day=(timestamp + timedelta(seconds=800)).day,
            user_id_modulo=1,
        ),
    ]


def generate_event_data_events_in_next_date(user_id: str, timestamp: datetime.timestamp):
    """
    Valid events in time point window, including an event from the following date which is the nearest event.
    Assumes the provided timestamp is near the end of the day AND only in the final time point's window.
    Expected result: event with cell id 2 at timestamp + timedelta(seconds=600) is included in the aggregated result
    """
    return [
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp - timedelta(seconds=800),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="1",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp - timedelta(seconds=800)).year,
            month=(timestamp - timedelta(seconds=800)).month,
            day=(timestamp - timedelta(seconds=800)).day,
            user_id_modulo=1,
        ),
        Row(
            user_id=user_id.encode("ascii"),
            timestamp=timestamp + timedelta(seconds=600),
            mcc=100,
            mnc=None,
            plmn=None,
            domain=Domains.DOMESTIC,
            cell_id="2",
            latitude=None,
            longitude=None,
            loc_error=None,
            error_flag=0,
            year=(timestamp + timedelta(seconds=600)).year,
            month=(timestamp + timedelta(seconds=600)).month,
            day=(timestamp + timedelta(seconds=600)).day,
            user_id_modulo=1,
        ),
    ]
