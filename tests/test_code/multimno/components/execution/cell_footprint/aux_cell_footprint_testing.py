import pytest
from configparser import ConfigParser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.sql import functions as F

from sedona.sql import st_constructors as STC

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from multimno.core.data_objects.silver.silver_signal_strength_data_object import SilverSignalStrengthDataObject
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)

from tests.test_code.test_common import TEST_GENERAL_CONFIG_PATH

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


@pytest.fixture
def expected_footprint(spark):
    """
    Aux function to setup expected data

    Args:
        spark (SparkSession): spark session
    """
    date_format = "%Y-%m-%d"
    date_start = datetime.strptime("2023-01-01", date_format).date()
    date_end = datetime.strptime("2023-01-02", date_format).date()

    expected_data = [
        Row(
            cell_id="641660723865491",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_dominance=0.7831298112869263,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="156659001067698",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_dominance=0.5404115319252014,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="216188987432445",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_dominance=0.1576259285211563,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    expected_sdf = spark.createDataFrame(expected_data, schema=SilverCellFootprintDataObject.SCHEMA)

    return expected_sdf


@pytest.fixture
def expected_intersection_groups(spark):
    """
    Aux function to setup expected data

    Args:
        spark (SparkSession): spark session
    """
    expected_data = [
        Row(
            group_id="3_1",
            cells=["156659001067698", "216188987432445", "641660723865491"],
            group_size=3,
            year=2023,
            month=1,
            day=1,
        ),
        Row(group_id="2_1", cells=["156659001067698", "216188987432445"], group_size=2, year=2023, month=1, day=1),
        Row(group_id="2_2", cells=["216188987432445", "641660723865491"], group_size=2, year=2023, month=1, day=1),
        Row(group_id="2_3", cells=["156659001067698", "641660723865491"], group_size=2, year=2023, month=1, day=1),
    ]

    expected_sdf = spark.createDataFrame(expected_data, schema=SilverCellIntersectionGroupsDataObject.SCHEMA)

    return expected_sdf


def set_input_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """

    date_format = "%Y-%m-%d"
    date_start = datetime.strptime("2023-01-01", date_format).date()
    date_end = datetime.strptime("2023-01-02", date_format).date()
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["signal_strength_data_silver"]

    # 4 cells with signal strength values in single grid tile
    # 1 cell signal strentgh is below the threshold and 3 are above.
    input_data = [
        Row(
            cell_id="641660723865491",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_strength=-86.08,
            distance_to_cell=1333.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="156659001067698",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_strength=-91.69,
            distance_to_cell=131.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="216188987432445",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_strength=-100.88,
            distance_to_cell=800.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="216188987432441",
            grid_id="100mN3039100E3910300",
            valid_date_start=date_start,
            valid_date_end=date_end,
            signal_strength=-160.88,
            distance_to_cell=430.0,
            year=2023,
            month=1,
            day=1,
        ),
    ]

    input_data_df = spark.createDataFrame(input_data, schema=SilverSignalStrengthDataObject.SCHEMA)
    ### Write input data in test resources dir
    input_data = SilverSignalStrengthDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)
