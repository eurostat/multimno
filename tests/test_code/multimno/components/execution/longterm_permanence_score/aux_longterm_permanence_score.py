import pytest
import datetime as dt
import calendar as cal
from configparser import ConfigParser
from multimno.core.constants.columns import ColNames
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row

from multimno.core.data_objects.silver.silver_midterm_permanence_score_data_object import (
    SilverMidtermPermanenceScoreDataObject,
)
from multimno.core.data_objects.silver.silver_longterm_permanence_score_data_object import (
    SilverLongtermPermanenceScoreDataObject,
)

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]

input_midterm_permanence_score_id = "input_midterm_permanence_score"
expected_output_longterm_permenence_score_id = "expected_longterm_permanence_score"


def get_expected_output_df(spark: SparkSession, expected_midterm_permanence_score_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame.
    Schema is SilverLongtermPermanenceScoreDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_midterm_permanence_score_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(
        expected_midterm_permanence_score_data, schema=SilverLongtermPermanenceScoreDataObject.SCHEMA
    )
    return expected_data_df


def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    midterm_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        event_data (list[Row]): list of event data rows
        cell_intersection_groups_data (list[Row]): list of cell intersection groups data rows
    """
    partition_columns = [
        ColNames.year,
        ColNames.month,
        ColNames.day_type,
        ColNames.time_interval,
        ColNames.id_type,
        ColNames.user_id_modulo,
    ]

    ### Write input midterm permanence data to test resources dir
    midterm_data_path = config["Paths.Silver"]["midterm_permanence_score_data_silver"]
    input_midterm_do = SilverMidtermPermanenceScoreDataObject(spark, midterm_data_path)
    input_midterm_do.df = spark.createDataFrame(
        midterm_data, schema=SilverMidtermPermanenceScoreDataObject.SCHEMA
    ).orderBy(ColNames.user_id, ColNames.year, ColNames.month)
    input_midterm_do.write(partition_columns=partition_columns)


def data_test_0001() -> dict:
    start_time = dt.datetime.strptime("2023-01", "%Y-%m").date()
    end_time = dt.datetime.strptime("2023-04", "%Y-%m")
    end_time += dt.timedelta(days=cal.monthrange(end_time.year, end_time.month)[1])
    end_time = end_time.date()

    # Generate midterm permanence score data and expected output data.
    # Note: currently expected output values are hand-set. It might be possible to automate them instead.
    input_midterm_data = []
    expected_output_data = []
    user_id = "1000".encode("ascii")
    input_midterm_data += generate_input_midterm_one_user_v1(user_id, start_time, end_time)
    expected_output_data += generate_expected_output_one_user_v1(user_id)
    user_id = "1001".encode("ascii")
    input_midterm_data += generate_input_midterm_one_user_v1(user_id, start_time, end_time)
    expected_output_data += generate_expected_output_one_user_v1(user_id)

    return {
        input_midterm_permanence_score_id: input_midterm_data,
        expected_output_longterm_permenence_score_id: expected_output_data,
    }


def generate_input_midterm_one_user_v1(user_id, start_month: dt.date, end_month: dt.date) -> list[Row]:
    """
    Generates mid-term permanence scores for the provided user for each month within the provided range.

    Variant 1.

    Categories generated:
        (all, all, grid),
        (all, night_time, grid),
        (workdays, working_hours, grid)
    """
    midterm_data = []
    grid_id = 1
    user_id_modulo = 0
    one_month = start_month
    while one_month < end_month:
        midterm_data.append(  # Add row of "all-all-grid"
            Row(
                user_id=user_id,
                grid_id=grid_id,
                mps=20,
                frequency=5,
                regularity_mean=0.5,
                regularity_std=0.2,
                year=one_month.year,
                month=one_month.month,
                day_type="all",
                time_interval="all",
                id_type="grid",
                user_id_modulo=user_id_modulo,
            ),
        )
        midterm_data.append(  # Add row of "all-night_time-grid"
            Row(
                user_id=user_id,
                grid_id=grid_id,
                mps=12,
                frequency=4,
                regularity_mean=0.4,
                regularity_std=0.3,
                year=one_month.year,
                month=one_month.month,
                day_type="all",
                time_interval="night_time",
                id_type="grid",
                user_id_modulo=user_id_modulo,
            ),
        )
        midterm_data.append(  # Add row of "workdays-working_hours-grid"
            Row(
                user_id=user_id,
                grid_id=grid_id,
                mps=11,
                frequency=3,
                regularity_mean=0.4,
                regularity_std=0.3,
                year=one_month.year,
                month=one_month.month,
                day_type="workdays",
                time_interval="working_hours",
                id_type="grid",
                user_id_modulo=user_id_modulo,
            ),
        )
        one_month += dt.timedelta(days=cal.monthrange(one_month.year, one_month.month)[1])
    return midterm_data


def generate_expected_output_one_user_v1(user_id) -> list[Row]:
    """
    Generate the list of expected output longterm permanence score values.

    Variant 1.
    """
    grid_id = 1
    user_id_modulo = 0
    expected_data = []

    # season="all"
    start_date = dt.datetime.strptime("2023-01", "%Y-%m").date()
    end_date = dt.datetime.strptime("2023-04", "%Y-%m")
    end_date += dt.timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    end_date = end_date.date()
    expected_data += [
        Row(
            user_id=user_id,
            grid_id=grid_id,
            lps=80,
            total_frequency=20,
            frequency_mean=5.0,
            frequency_std=0.0,
            regularity_mean=0.5,
            regularity_std=0.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type="grid",
            user_id_modulo=user_id_modulo,
        ),
        Row(
            user_id=user_id,
            grid_id=grid_id,
            lps=48,
            total_frequency=16,
            frequency_mean=4.0,
            frequency_std=0.0,
            regularity_mean=0.4,
            regularity_std=0.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="night_time",
            id_type="grid",
            user_id_modulo=user_id_modulo,
        ),
    ]

    # season="winter"
    start_date = dt.datetime.strptime("2023-01", "%Y-%m").date()
    end_date = dt.datetime.strptime("2023-02", "%Y-%m")
    end_date += dt.timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    end_date = end_date.date()
    expected_data += [
        Row(
            user_id=user_id,
            grid_id=grid_id,
            lps=40,
            total_frequency=10,
            frequency_mean=5.0,
            frequency_std=0.0,
            regularity_mean=0.5,
            regularity_std=0.0,
            season="winter",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type="grid",
            user_id_modulo=user_id_modulo,
        ),
    ]

    # season="spring"
    start_date = dt.datetime.strptime("2023-03", "%Y-%m").date()
    end_date = dt.datetime.strptime("2023-04", "%Y-%m")
    end_date += dt.timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    end_date = end_date.date()
    expected_data += [
        Row(
            user_id=user_id,
            grid_id=grid_id,
            lps=40,
            total_frequency=10,
            frequency_mean=5.0,
            frequency_std=0.0,
            regularity_mean=0.5,
            regularity_std=0.0,
            season="spring",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type="grid",
            user_id_modulo=user_id_modulo,
        ),
        Row(
            user_id=user_id,
            grid_id=grid_id,
            lps=22,
            total_frequency=6,
            frequency_mean=3.0,
            frequency_std=0.0,
            regularity_mean=0.4,
            regularity_std=0.0,
            season="spring",
            start_date=start_date,
            end_date=end_date,
            day_type="workdays",
            time_interval="working_hours",
            id_type="grid",
            user_id_modulo=user_id_modulo,
        ),
    ]
    return expected_data
