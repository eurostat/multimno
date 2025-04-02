import datetime as dt
from multimno.core.constants.columns import ColNames
from pyspark.sql import Row
from pyspark.sql import SparkSession
from configparser import ConfigParser

from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import SilverGeozonesGridMapDataObject
from multimno.core.utils import calc_hashed_user_id, apply_schema_casting


def generate_input_ue_labels_previous_data() -> list[Row]:
    """Generate the UE labels for the first LT period input data for testing purposes.

    Returns:
        list[Row]: list of rows that form the input output.
    """
    input_data = [
        Row(
            **{
                "user_id": 1,
                "grid_id": 1,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 2,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 4,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 3,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 6,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 5,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 8,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 7,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 2,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 4,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 3,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 8,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 1,
                "label": "home",
                ColNames.label_rule: "ue_1",
                ColNames.id_type: "h_1",
                "start_date": dt.datetime(2023, 1, 1).date(),
                "end_date": dt.datetime(2023, 6, 30).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
    ]

    return input_data


def generate_input_ue_labels_new_data() -> list[Row]:
    """Generate the UE labels for the second LT period input data for testing purposes.

    Returns:
        list[Row]: list of rows that form the input output.
    """

    input_data = [
        Row(
            **{
                "user_id": 1,
                "grid_id": 9,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 2,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 4,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 11,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 12,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 5,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 1,
                "grid_id": 1,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 3,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 2,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 9,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
        Row(
            **{
                "user_id": 2,
                "grid_id": 10,
                "label": "home",
                ColNames.label_rule: "ue_2",
                ColNames.id_type: "h_2",
                "start_date": dt.datetime(2023, 7, 1).date(),
                "end_date": dt.datetime(2023, 12, 31).date(),
                "season": "all",
                "user_id_modulo": 0,
            }
        ),
    ]

    return input_data


def generate_input_geozones_grid_map_data() -> list[Row]:
    """Generate the grid to zone mapping input data for testing purposes.

    Returns:
        list[Row]: list of rows that form the input output.
    """
    input_data = [
        Row(
            **{
                "grid_id": 10,
                "zone_id": "3",
                "hierarchical_id": "3",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 9,
                "zone_id": "3",
                "hierarchical_id": "3",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 11,
                "zone_id": "3",
                "hierarchical_id": "3",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 12,
                "zone_id": "3",
                "hierarchical_id": "3",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 2,
                "zone_id": "1",
                "hierarchical_id": "1",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 3,
                "zone_id": "1",
                "hierarchical_id": "1",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 4,
                "zone_id": "1",
                "hierarchical_id": "1",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 1,
                "zone_id": "2",
                "hierarchical_id": "2",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 5,
                "zone_id": "2",
                "hierarchical_id": "2",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 6,
                "zone_id": "2",
                "hierarchical_id": "2",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 7,
                "zone_id": "2",
                "hierarchical_id": "2",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
        Row(
            **{
                "grid_id": 8,
                "zone_id": "2",
                "hierarchical_id": "2",
                "dataset_id": "nuts",
                "year": 2023,
                "month": 1,
                "day": 8,
            }
        ),
    ]

    return input_data


def set_input_data(spark: SparkSession, config: ConfigParser):
    previous_ue_labels = generate_input_ue_labels_previous_data()
    new_ue_labels = generate_input_ue_labels_new_data()
    geo = generate_input_geozones_grid_map_data()

    prev_ue_labels_do = SilverUsualEnvironmentLabelsDataObject(
        spark, config["Paths.Silver"]["aggregated_usual_environments_silver"]
    )
    new_ue_labels_do = SilverUsualEnvironmentLabelsDataObject(
        spark, config["Paths.Silver"]["aggregated_usual_environments_silver"]
    )
    geozones_do = SilverGeozonesGridMapDataObject(spark, config["Paths.Silver"]["geozones_grid_map_data_silver"])

    prev_ue_labels_do.df = apply_schema_casting(
        calc_hashed_user_id(spark.createDataFrame(previous_ue_labels)),
        schema=SilverUsualEnvironmentLabelsDataObject.SCHEMA,
    )
    new_ue_labels_do.df = apply_schema_casting(
        calc_hashed_user_id(spark.createDataFrame(new_ue_labels)), schema=SilverUsualEnvironmentLabelsDataObject.SCHEMA
    )
    geozones_do.df = spark.createDataFrame(geo, schema=SilverGeozonesGridMapDataObject.SCHEMA)

    prev_ue_labels_do.write()
    new_ue_labels_do.write()
    geozones_do.write()


def generate_expected_internal_migration_data() -> list[Row]:
    """Generate the expected output of the test of internal migration.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    expected = [
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

    return expected


def generate_expected_internal_migration_quality_metrics() -> list[Row]:
    """Generate the expected quality metrics of the test of internal migration.

    Returns:
        list[Row]: list of rows that form the expected output.
    """
    expected = [
        {
            "previous_home_users": 2,
            "new_home_users": 2,
            "common_home_users": 2,
            "result_timestamp": dt.datetime(2024, 12, 23, 11, 39, 56, 338401),
            "dataset_id": "nuts",
            "level": 1,
            "start_date_previous": dt.datetime(2023, 1, 1).date(),
            "end_date_previous": dt.datetime(2023, 6, 30).date(),
            "season_previous": "all",
            "start_date_new": dt.datetime(2023, 7, 1).date(),
            "end_date_new": dt.datetime(2023, 12, 31).date(),
            "season_new": "all",
        }
    ]
    return expected
