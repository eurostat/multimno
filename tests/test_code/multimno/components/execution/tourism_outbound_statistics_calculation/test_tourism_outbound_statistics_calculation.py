import os
import shutil
import pytest
from multimno.components.execution.tourism_outbound_statistics.tourism_outbound_statistics_calculation import (
    TourismOutboundStatisticsCalculation,
)
from multimno.core.data_objects.silver.silver_tourism_outbound_nights_spent_data_object import (
    SilverTourismOutboundNightsSpentDataObject,
)
from multimno.core.data_objects.silver.silver_tourism_trip_data_object import SilverTourismTripDataObject
from multimno.core.spark_session import delete_file_or_folder
from multimno.core.utils import apply_schema_casting
from pyspark.testing.utils import assertDataFrameEqual
import pyspark.sql.functions as F
from multimno.core.configuration import parse_configuration
from multimno.core.constants.columns import ColNames

from tests.test_code.fixtures import spark_session as spark

from tests.test_code.multimno.components.execution.tourism_outbound_statistics_calculation.aux_tourism_outbound_statistics_calculation import (
    set_input_data,
    get_expected_trips_output_df,
    get_expected_agg_output_df,
)
from tests.test_code.multimno.components.execution.tourism_outbound_statistics_calculation.aux_tourism_outbound_statistics_calculation import (
    input_time_segments_id,
    input_tourism_trips_id,
    expected_tourism_trips_id,
    expected_tourism_outbound_nights_spent_id,
)
from tests.test_code.multimno.components.execution.tourism_outbound_statistics_calculation.aux_tourism_outbound_statistics_calculation import (
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


@pytest.mark.parametrize("get_test_data", [data_test_0001])
def test_tourism_outbound_statistics_calculation(spark, get_test_data):
    # Setup
    test_data_dict = get_test_data()
    ## Init configs & paths
    component_config_path = f"{TEST_RESOURCES_PATH}/config/tourism_outbound_statistics_calculation/testing_tourism_outbound_statistics_calculation.ini"
    config = parse_configuration(TEST_GENERAL_CONFIG_PATH, component_config_path)

    ## Delete any existing tourism trips data before writing test input data
    delete_file_or_folder(spark, config["Paths.Silver"][input_tourism_trips_id])

    ## Create Input data
    set_input_data(
        spark,
        config,
        test_data_dict[input_time_segments_id],
        test_data_dict[input_tourism_trips_id],
    )

    ## Init component class
    tourism_aggregation = TourismOutboundStatisticsCalculation(TEST_GENERAL_CONFIG_PATH, component_config_path)

    # Execution
    tourism_aggregation.execute()

    # Assert trip equality
    output_trips_data_object = tourism_aggregation.output_data_objects[SilverTourismTripDataObject.ID]
    output_trips_data_object.read()
    output_trips_df = output_trips_data_object.df
    # output_trips_df.orderBy([ColNames.user_id, ColNames.trip_start_timestamp]).show(truncate=False)
    expected_trips = get_expected_trips_output_df(spark, test_data_dict[expected_tourism_trips_id])
    assertDataFrameEqual(output_trips_df, expected_trips)

    # Assert aggregations equality
    output_agg_data_object = tourism_aggregation.output_data_objects[SilverTourismOutboundNightsSpentDataObject.ID]
    output_agg_data_object.read()
    output_agg_df = output_agg_data_object.df
    # output_agg_df.orderBy(ColNames.time_period).show(truncate=False)
    expected_agg_df = get_expected_agg_output_df(spark, test_data_dict[expected_tourism_outbound_nights_spent_id])
    expected_agg_df = apply_schema_casting(expected_agg_df, output_agg_df.schema)
    assertDataFrameEqual(output_agg_df, expected_agg_df)
