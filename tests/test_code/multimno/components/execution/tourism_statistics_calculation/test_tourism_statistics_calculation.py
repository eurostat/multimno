import os
import shutil
import pytest
from multimno.components.execution.tourism_statistics.tourism_statistics_calculation import TourismStatisticsCalculation
from multimno.core.data_objects.silver.silver_tourism_trip_avg_destinations_nights_spent_data_object import (
    SilverTourismTripAvgDestinationsNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject
from multimno.core.data_objects.silver.silver_tourism_zone_departures_nights_spent_data_object import (
    SilverTourismZoneDeparturesNightsSpentDataObject,
)
from multimno.core.spark_session import delete_file_or_folder
from multimno.core.utils import apply_schema_casting
from pyspark.testing.utils import assertDataFrameEqual
import pyspark.sql.functions as F
from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.multimno.components.execution.tourism_statistics_calculation.aux_tourism_statistics_calculation import (
    data_test_0002,
    set_input_data,
    get_expected_trips_output_df,
    get_expected_agg_i_output_df,
    get_expected_agg_ii_output_df,
)
from tests.test_code.multimno.components.execution.tourism_statistics_calculation.aux_tourism_statistics_calculation import (
    input_tourism_stays_id,
    input_tourism_trips_id,
    input_mcc_iso_tz_map_id,
    expected_tourism_trips_id,
    expected_tourism_geozone_aggregations_id,
    expected_tourism_trip_aggregations_id,
)
from tests.test_code.multimno.components.execution.tourism_statistics_calculation.aux_tourism_statistics_calculation import (
    data_test_0001,
)

from tests.test_code.test_common import TEST_RESOURCES_PATH, TEST_GENERAL_CONFIG_PATH
from tests.test_code.test_utils import setup_test_data_dir, teardown_test_data_dir


# Dummy to avoid linting errors using pytest
fixtures = [spark]


def setup_function():
    setup_test_data_dir()


def teardown_function():
    teardown_test_data_dir()


@pytest.mark.parametrize("get_test_data", [data_test_0001, data_test_0002])
def test_tourism_statistics_calculation(spark, get_test_data):
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = (
        f"{TEST_RESOURCES_PATH}/config/tourism_statistics_calculation/testing_tourism_statistics_calculation.ini"
    )
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Delete any existing tourism trips data before writing test input data
    delete_file_or_folder(spark, config["Paths.Silver"][input_tourism_trips_id])

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_tourism_stays_id],
        test_data_dict[input_tourism_trips_id],
        test_data_dict[input_mcc_iso_tz_map_id],
    )

    ## Init component class
    tourism_aggregation = TourismStatisticsCalculation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    tourism_aggregation.execute()

    # Assert trip equality
    output_trips_data_object = tourism_aggregation.output_data_objects[SilverTourismTripDataObject.ID]
    output_trips_data_object.read()
    output_trips_df = output_trips_data_object.df
    output_trips_df.orderBy([ColNames.user_id, ColNames.trip_start_timestamp]).show(truncate=False)
    expected_trips = get_expected_trips_output_df(spark, test_data_dict[expected_tourism_trips_id])
    expected_trips.orderBy([ColNames.user_id, ColNames.trip_start_timestamp]).show(truncate=False)
    assertDataFrameEqual(output_trips_df, expected_trips)

    # Assert aggregations I equality
    output_agg_i_data_object = tourism_aggregation.output_data_objects[
        SilverTourismZoneDeparturesNightsSpentDataObject.ID
    ]
    output_agg_i_data_object.read()
    output_agg_i_df = output_agg_i_data_object.df
    # output_agg_i_df.orderBy(ColNames.time_period, ColNames.level, ColNames.zone_id).show(truncate=False)
    expected_agg_i = get_expected_agg_i_output_df(spark, test_data_dict[expected_tourism_geozone_aggregations_id])
    expected_agg_i = apply_schema_casting(expected_agg_i, output_agg_i_df.schema)
    assertDataFrameEqual(output_agg_i_df, expected_agg_i)

    # Assert aggregations II equality
    output_agg_ii_data_object = tourism_aggregation.output_data_objects[
        SilverTourismTripAvgDestinationsNightsSpentDataObject.ID
    ]
    output_agg_ii_data_object.read()
    output_agg_ii_df = output_agg_ii_data_object.df
    # output_agg_ii_df.orderBy(ColNames.time_period, ColNames.level, ColNames.mcc).show(truncate=False)
    expected_agg_ii = get_expected_agg_ii_output_df(spark, test_data_dict[expected_tourism_trip_aggregations_id])
    expected_agg_ii = apply_schema_casting(expected_agg_ii, output_agg_ii_df.schema)
    assertDataFrameEqual(output_agg_ii_df, expected_agg_ii)
