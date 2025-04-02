# TODO convert to cell prox estimation

import csv
from io import StringIO
from configparser import ConfigParser
from typing import List
from multimno.core.data_objects.silver.silver_cell_footprint_data_object import SilverCellFootprintDataObject
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_intersection_groups_data_object import (
    SilverCellIntersectionGroupsDataObject,
)
from multimno.core.data_objects.silver.silver_cell_distance_data_object import SilverCellDistanceDataObject

from tests.test_code.fixtures import spark_session as spark

# Dummy to avoid linting errors using pytest
fixtures = [spark]


def set_input_data(spark: SparkSession, config: ConfigParser):
    """
    Aux function to setup input data

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
    """
    partition_columns = [ColNames.year, ColNames.month, ColNames.day]
    test_data_path = config["Paths.Silver"]["cell_footprint_data_silver"]

    input_data = get_input_cell_footprint_data()
    input_data_df = spark.createDataFrame(input_data, SilverCellFootprintDataObject.SCHEMA)
    ### Write input data in test resources dir
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)


def get_input_cell_footprint_data() -> List[Row]:
    data = [
        # grid id example: 100mN3092200E3928900
        # cA, 2023-01-01 (near cB, cC)
        Row(
            cell_id="cA",
            grid_id=655370,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cA",
            grid_id=720906,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cA",
            grid_id=655371,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cB, 2023-01-01 (near cA, cC)
        Row(
            cell_id="cB",
            grid_id=655372,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cB",
            grid_id=655373,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cC, 2023-01-01 (near cA, cB)
        Row(
            cell_id="cC",
            grid_id=655373,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cD, 2023-01-01 (near cE)
        Row(
            cell_id="cD",
            grid_id=32768200,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cE, 2023-01-01 (near cD)
        Row(
            cell_id="cE",
            grid_id=32899274,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cA, 2023-01-03 (same as previous 2023-01-01)
        Row(
            cell_id="cA",
            grid_id=655370,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cA",
            grid_id=720906,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cA",
            grid_id=655371,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        # cB, 2023-01-03 (different from prev 2023-01-01)
        Row(
            cell_id="cB",
            grid_id=720908,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cB",
            grid_id=655373,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        # cC, 2023-01-03 (far from previous location)
        Row(
            cell_id="cC",
            grid_id=262148000,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
    ]
    return data


def get_expected_cell_intersection_groups_df() -> List[Row]:
    data = [
        # cA 2023-01-01
        Row(cell_id="cA", overlapping_cell_ids=["cB", "cC"], year=2023, month=1, day=1),
        # cB 2023-01-01
        Row(cell_id="cB", overlapping_cell_ids=["cA", "cC"], year=2023, month=1, day=1),
        # cC 2023-01-01
        Row(cell_id="cC", overlapping_cell_ids=["cA", "cB"], year=2023, month=1, day=1),
        # cD 2023-01-01
        Row(cell_id="cD", overlapping_cell_ids=[], year=2023, month=1, day=1),
        # cE 2023-01-01
        Row(cell_id="cE", overlapping_cell_ids=[], year=2023, month=1, day=1),
        # cA 2023-01-03
        Row(cell_id="cA", overlapping_cell_ids=["cB"], year=2023, month=1, day=3),
        # cB 2023-01-03
        Row(cell_id="cB", overlapping_cell_ids=["cA"], year=2023, month=1, day=3),
        # cC 2023-01-03
        Row(cell_id="cC", overlapping_cell_ids=[], year=2023, month=1, day=3),
    ]
    return data


def get_expected_output_cell_distance_df() -> List[Row]:

    expected = [
        Row(cell_id_a="cC", cell_id_b="cB", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cC", cell_id_b="cA", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cA", cell_id_b="cB", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cA", cell_id_b="cC", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cB", cell_id_b="cA", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cB", cell_id_b="cC", distance=0.0, year=2023, month=1, day=1),
        Row(cell_id_a="cB", cell_id_b="cA", distance=0.0, year=2023, month=1, day=3),
        Row(cell_id_a="cA", cell_id_b="cB", distance=0.0, year=2023, month=1, day=3),
        Row(cell_id_a="cD", cell_id_b="cE", distance=82.8427124023, year=2023, month=1, day=1),
        Row(cell_id_a="cE", cell_id_b="cD", distance=82.8427124023, year=2023, month=1, day=1),
    ]

    return expected
