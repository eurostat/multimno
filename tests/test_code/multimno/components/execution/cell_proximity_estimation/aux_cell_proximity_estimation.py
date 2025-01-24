# TODO convert to cell prox estimation

import csv
from io import StringIO
import pytest
from configparser import ConfigParser
from datetime import datetime
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

    input_data_df = get_input_cell_footprint(spark)
    ### Write input data in test resources dir
    input_data = SilverCellFootprintDataObject(spark, test_data_path)
    input_data.df = input_data_df
    input_data.write(partition_columns=partition_columns)


def get_input_cell_footprint(spark):
    data = [
        # grid id example: 100mN3092200E3928900
        # cA, 2023-01-01 (near cB, cC)
        Row(
            cell_id="cA",
            grid_id=10000101000010,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cA",
            grid_id=10000111000010,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cA",
            grid_id=10000101000011,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cB, 2023-01-01 (near cA, cC)
        Row(
            cell_id="cB",
            grid_id=10000101000012,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        Row(
            cell_id="cB",
            grid_id=10000101000013,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cC, 2023-01-01 (near cA, cB)
        Row(
            cell_id="cC",
            grid_id=10000101000013,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cD, 2023-01-01 (near cE)
        Row(
            cell_id="cD",
            grid_id=10005001000200,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cE, 2023-01-01 (near cD)
        Row(
            cell_id="cE",
            grid_id=10005051000205,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=1,
        ),
        # cA, 2023-01-03 (same as previous 2023-01-01)
        Row(
            cell_id="cA",
            grid_id=10000101000010,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cA",
            grid_id=10000111000010,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cA",
            grid_id=10000101000011,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        # cB, 2023-01-03 (different from prev 2023-01-01)
        Row(
            cell_id="cB",
            grid_id=10000111000012,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        Row(
            cell_id="cB",
            grid_id=10000101000013,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
        # cC, 2023-01-03 (far from previous location)
        Row(
            cell_id="cC",
            grid_id=10040001004000,
            signal_dominance=1.0,
            year=2023,
            month=1,
            day=3,
        ),
    ]
    return spark.createDataFrame(data, schema=SilverCellFootprintDataObject.SCHEMA)


def get_expected_cell_intersection_groups_df(spark):
    data = [
        # cA 2023-01-01
        Row(cell_id="cA", overlapping_cell_ids=["cB", "cC"], year=2023, month=1, day=1),
        # cB 2023-01-01
        Row(cell_id="cB", overlapping_cell_ids=["cA", "cC"], year=2023, month=1, day=1),
        # cC 2023-01-01
        Row(cell_id="cC", overlapping_cell_ids=["cA", "cB"], year=2023, month=1, day=1),
        # cD 2023-01-01
        Row(cell_id="cD", overlapping_cell_ids=["cE"], year=2023, month=1, day=1),
        # cE 2023-01-01
        Row(cell_id="cE", overlapping_cell_ids=["cD"], year=2023, month=1, day=1),
        # cA 2023-01-03
        Row(cell_id="cA", overlapping_cell_ids=["cB"], year=2023, month=1, day=3),
        # cB 2023-01-03
        Row(cell_id="cB", overlapping_cell_ids=["cA"], year=2023, month=1, day=3),
        # cC 2023-01-03
        Row(cell_id="cC", overlapping_cell_ids=[], year=2023, month=1, day=3),
    ]
    return spark.createDataFrame(data, schema=SilverCellIntersectionGroupsDataObject.SCHEMA)


def get_expected_output_cell_distance_df(spark):
    data_str = """cell_id_a|cell_id_b| distance|year|month|day
                        cC|       cB|      0.0|2023|    1|  1
                        cC|       cE|331.01474|2023|    1|  1
                        cC|       cA|      0.0|2023|    1|  1
                        cC|       cD|324.59805|2023|    1|  1
                        cE|       cB|331.01474|2023|    1|  1
                        cE|       cA| 331.1391|2023|    1|  1
                        cE|       cC|331.01474|2023|    1|  1
                        cE|       cD|      0.0|2023|    1|  1
                        cA|       cB|      0.0|2023|    1|  1
                        cA|       cE| 331.1391|2023|    1|  1
                        cA|       cC|      0.0|2023|    1|  1
                        cA|       cD|324.69403|2023|    1|  1
                        cB|       cE|331.01474|2023|    1|  1
                        cB|       cA|      0.0|2023|    1|  1
                        cB|       cC|      0.0|2023|    1|  1
                        cB|       cD|324.59805|2023|    1|  1
                        cD|       cB|324.59805|2023|    1|  1
                        cD|       cE|      0.0|2023|    1|  1
                        cD|       cA|324.69403|2023|    1|  1
                        cD|       cC|324.59805|2023|    1|  1
                        cB|       cA|      0.0|2023|    1|  3
                        cA|       cB|      0.0|2023|    1|  3"""
    csv_input = csv.reader(StringIO(data_str, newline="\n"), delimiter="|")
    next(csv_input, None)  # Skip header
    data = []
    for row in csv_input:
        data.append(
            Row(
                cell_id_a=row[0].strip(),
                cell_id_b=row[1].strip(),
                distance=float(row[2].strip()),
                year=int(row[3].strip()),
                month=int(row[4].strip()),
                day=int(row[5].strip()),
            )
        )
    return spark.createDataFrame(data, schema=SilverCellDistanceDataObject.SCHEMA)
