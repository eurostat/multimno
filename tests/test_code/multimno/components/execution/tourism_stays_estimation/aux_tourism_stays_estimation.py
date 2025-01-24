from configparser import ConfigParser
from datetime import datetime
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.silver.silver_cell_connection_probabilities_data_object import (
    SilverCellConnectionProbabilitiesDataObject,
)
from multimno.core.data_objects.silver.silver_geozones_grid_map_data_object import SilverGeozonesGridMapDataObject
from multimno.core.data_objects.silver.silver_usual_environment_labels_data_object import (
    SilverUsualEnvironmentLabelsDataObject,
)
from multimno.core.data_objects.silver.silver_time_segments_data_object import SilverTimeSegmentsDataObject
from multimno.core.data_objects.silver.silver_tourism_stays_data_object import SilverTourismStaysDataObject
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import Row
from tests.test_code.fixtures import spark_session as spark


input_time_segments_id = "input_time_segments"
input_cell_connection_probabilities_id = "input_cell_connection_probabilities"
input_geozones_grid_mapping_id = "input_geozones_grid_mapping"
input_ue_labels_id = "input_ue_labels"
expected_tourism_stays_id = "expected_tourism_stays"


# TODO
def get_expected_output_df(spark: SparkSession, expected_tourism_stays_data: list[Row]) -> DataFrame:
    """Function to turn provided expected result data into Spark DataFrame. Schema is SilverTourismStaysDataObject.SCHEMA.

    Args:
        spark (SparkSession): Spark session
        expected_tourism_stays_data (list[Row]): list of Rows matching the schema

    Returns:
        DataFrame: Spark DataFrame containing provided rows
    """
    expected_data_df = spark.createDataFrame(expected_tourism_stays_data, schema=SilverTourismStaysDataObject.SCHEMA)
    return expected_data_df


# TODO
def set_input_data(
    spark: SparkSession,
    config: ConfigParser,
    time_segments_data: list[Row],
    cell_connection_probabilities_data: list[Row],
    geozones_grid_map_data: list[Row],
    ue_labels_data: list[Row],
):
    """
    Function to write test-specific provided dataframes to input directories.

    Args:
        spark (SparkSession): spark session
        config (ConfigParser): component config
        time_segments_data (list[Row]): list of time segments data rows
        cell_connection_probabilities_data (list[Row]): list of cell to grid connection probabilities data rows
        geozones_grid_map_data (list[Row]): list of geozone to grid mapping data rows
    """

    ### Write input time segments data to test resources dir
    time_segments_data_path = config["Paths.Silver"]["time_segments_silver"]
    input_time_segments_do = SilverTimeSegmentsDataObject(spark, time_segments_data_path)
    input_time_segments_do.df = spark.createDataFrame(
        time_segments_data, schema=SilverTimeSegmentsDataObject.SCHEMA
    ).orderBy(ColNames.user_id, ColNames.start_timestamp)
    input_time_segments_do.write()

    ### Write input cell connection probabilities data to test resources dir
    cell_connection_probabilities_data_path = config["Paths.Silver"]["cell_connection_probabilities_data_silver"]
    cell_connection_probabilities_do = SilverCellConnectionProbabilitiesDataObject(
        spark, cell_connection_probabilities_data_path
    )
    cell_connection_probabilities_do.df = spark.createDataFrame(
        cell_connection_probabilities_data, schema=SilverCellConnectionProbabilitiesDataObject.SCHEMA
    )
    cell_connection_probabilities_do.write()

    ### Write input geozones to grid mapping data to test resources dir
    geozones_grid_map_data_path = config["Paths.Silver"]["geozones_grid_map_data_silver"]
    geozones_grid_map_do = SilverGeozonesGridMapDataObject(spark, geozones_grid_map_data_path)
    geozones_grid_map_do.df = spark.createDataFrame(
        geozones_grid_map_data, schema=SilverGeozonesGridMapDataObject.SCHEMA
    )
    geozones_grid_map_do.write()

    ### Write input usual environment labels data to test resources dir
    ue_labels_data_path = config["Paths.Silver"]["usual_environment_labels_data_silver"]
    ue_labels_do = SilverUsualEnvironmentLabelsDataObject(spark, ue_labels_data_path)
    ue_labels_do.df = spark.createDataFrame(ue_labels_data, schema=SilverUsualEnvironmentLabelsDataObject.SCHEMA)
    ue_labels_do.write()


# TODO
def data_test_0001() -> dict:
    # Test case: one user. Combination of stay, move, undetermined, unknown segments.
    date_format = "%Y-%m-%dT%H:%M:%S"
    cell_id_a = "a0001"
    cell_id_b1 = "b0001"
    cell_id_b2 = "b0002"
    user_id = "1000".encode("ascii")
    mcc = 100
    mnc = "01"
    plmn = 0
    year = 2023
    month = 1
    day = 3
    user_id_modulo = 0

    input_input_ue_labels_data = [
        Row(
            user_id="2000".encode("ascii"),
            grid_id=10001,
            label="ue",
            ue_label_rule="rule1",
            location_label_rule="rule2",
            start_date=datetime.strptime("2023-01-01", "%Y-%m-%d"),
            end_date=datetime.strptime("2023-06-30", "%Y-%m-%d"),
            season="all",
            user_id_modulo=1,
        ),
    ]

    input_time_segments_data = [
        Row(  # "unknown" segment for entire date before first events. Should get filtered out.
            user_id=user_id,
            time_segment_id=1,
            start_timestamp=datetime.strptime("2023-01-02T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-02T23:59:59", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=True,
            year=year,
            month=month,
            day=day - 1,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # "unknown" segment from start of day until first event. End is shortened by padding. Should get filtered out.
            user_id=user_id,
            time_segment_id=1,
            start_timestamp=datetime.strptime("2023-01-03T00:00:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[],
            state="unknown",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_a. Should get included.
            user_id=user_id,
            time_segment_id=2,
            start_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T05:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="stay",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_a of an inbound resident Should get excluded.
            user_id="2000".encode("ascii"),
            time_segment_id=2,
            start_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T13:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="stay",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Move section. Should get filtered out.
            user_id=user_id,
            time_segment_id=3,
            start_timestamp=datetime.strptime("2023-01-03T04:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T05:54:30", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="move",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_b1,cell_id_b2. Should get included.
            user_id=user_id,
            time_segment_id=4,
            start_timestamp=datetime.strptime("2023-01-03T05:54:30", date_format),
            end_timestamp=datetime.strptime("2023-01-03T09:45:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_b1, cell_id_b2],
            state="stay",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_a that is short. Should get filtered out.
            user_id=user_id,
            time_segment_id=5,
            start_timestamp=datetime.strptime("2023-01-03T10:05:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T11:25:00", date_format),
            mcc=mcc,
            mnc=mnc,
            plmn=plmn,
            cells=[cell_id_a],
            state="stay",
            is_last=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
    ]

    grid_id_1 = 10001
    grid_id_2 = 10002
    grid_id_3 = 10003
    grid_id_4 = 10004
    grid_id_5 = 10005

    input_cell_connection_probabilities_data = [
        # cell_id_a has grids 1, 2
        Row(
            cell_id=cell_id_a,
            grid_id=grid_id_1,
            cell_connection_probability=0.4,
            posterior_probability=0.4,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_a,
            grid_id=grid_id_2,
            cell_connection_probability=0.6,
            posterior_probability=0.6,
            year=year,
            month=month,
            day=day,
        ),
        # cell_id_b1 has grids 3,4, some overlapping with cell_id_b2
        Row(
            cell_id=cell_id_b1,
            grid_id=grid_id_3,
            cell_connection_probability=0.3,
            posterior_probability=0.3,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_b1,
            grid_id=grid_id_4,
            cell_connection_probability=0.7,
            posterior_probability=0.7,
            year=year,
            month=month,
            day=day,
        ),
        # cell_id_b2 has grids 3,4,5, some overlapping with cell_id_b1
        Row(
            cell_id=cell_id_b2,
            grid_id=grid_id_3,
            cell_connection_probability=0.15,
            posterior_probability=0.15,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_b2,
            grid_id=grid_id_4,
            cell_connection_probability=0.45,
            posterior_probability=0.45,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            cell_id=cell_id_b2,
            grid_id=grid_id_5,
            cell_connection_probability=0.4,
            posterior_probability=0.4,
            year=year,
            month=month,
            day=day,
        ),
    ]

    zone_id_1 = "z001"
    hierarchical_id_1 = "z0|z00|z001"
    zone_id_2 = "z002"
    hierarchical_id_2 = "z0|z00|z002"
    zone_id_3 = "z003"
    hierarchical_id_3 = "z0|z00|z003"

    dataset_id = "test_dataset"

    input_geozones_grid_mapping_data = [
        # zone 1 contains grid 1,2
        Row(
            grid_id=grid_id_1,
            zone_id=zone_id_1,
            hierarchical_id=hierarchical_id_1,
            dataset_id=dataset_id,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            grid_id=grid_id_2,
            zone_id=zone_id_1,
            hierarchical_id=hierarchical_id_1,
            dataset_id=dataset_id,
            year=year,
            month=month,
            day=day,
        ),
        # zone 2 contains grids 3,4
        Row(
            grid_id=grid_id_3,
            zone_id=zone_id_2,
            hierarchical_id=hierarchical_id_2,
            dataset_id=dataset_id,
            year=year,
            month=month,
            day=day,
        ),
        Row(
            grid_id=grid_id_4,
            zone_id=zone_id_2,
            hierarchical_id=hierarchical_id_2,
            dataset_id=dataset_id,
            year=year,
            month=month,
            day=day,
        ),
        # zone 3 contains grids 5
        Row(
            grid_id=grid_id_5,
            zone_id=zone_id_3,
            hierarchical_id=hierarchical_id_3,
            dataset_id=dataset_id,
            year=year,
            month=month,
            day=day,
        ),
    ]

    expected_tourism_stays_data = [
        Row(  # Stay at cell_id_a. Should get included.
            user_id=user_id,
            time_segment_id="2",
            start_timestamp=datetime.strptime("2023-01-03T00:55:00", date_format),
            end_timestamp=datetime.strptime("2023-01-03T05:55:00", date_format),
            mcc=mcc,
            mnc=mnc,
            zone_ids_list=["z0|z00|z001"],
            zone_weights_list=[1.0],
            is_overnight=True,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
        Row(  # Stay at cell_id_b1,cell_id_b2. Should get included.
            user_id=user_id,
            time_segment_id="4",
            start_timestamp=datetime.strptime("2023-01-03T05:54:30", date_format),
            end_timestamp=datetime.strptime("2023-01-03T09:45:00", date_format),
            mcc=mcc,
            mnc=mnc,
            zone_ids_list=["z0|z00|z002", "z0|z00|z003"],
            zone_weights_list=[0.8, 0.2],
            is_overnight=False,
            year=year,
            month=month,
            day=day,
            user_id_modulo=user_id_modulo,
        ),
    ]

    return {
        input_time_segments_id: input_time_segments_data,
        input_cell_connection_probabilities_id: input_cell_connection_probabilities_data,
        input_geozones_grid_mapping_id: input_geozones_grid_mapping_data,
        input_ue_labels_id: input_input_ue_labels_data,
        expected_tourism_stays_id: expected_tourism_stays_data,
    }
