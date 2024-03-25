"""
Reusable internal column names. Useful for referring to the the same column across multiple components.
"""


class ColNames:
    """
    Class that enumerates all the column names.
    """

    user_id = "user_id"
    partition_id = "partition_id"
    timestamp = "timestamp"
    mcc = "mcc"
    cell_id = "cell_id"
    latitude = "latitude"
    longitude = "longitude"
    error_flag = "error_flag"

    altitude = "altitude"
    antenna_height = "antenna_height"
    directionality = "directionality"
    azimuth_angle = "azimuth_angle"
    elevation_angle = "elevation_angle"
    horizontal_beam_width = "horizontal_beam_width"
    vertical_beam_width = "vertical_beam_width"
    power = "power"
    frequency = "frequency"
    technology = "technology"
    valid_date_start = "valid_date_start"
    valid_date_end = "valid_date_end"
    cell_type = "cell_type"

    loc_error = "loc_error"
    event_id = "event_id"

    year = "year"
    month = "month"
    day = "day"
    user_id_modulo = "user_id_modulo"

    # for QA by column
    variable = "variable"
    type_of_error = "type_of_error"
    type_of_transformation = "type_of_transformation"
    value = "value"
    result_timestamp = "result_timestamp"
    data_period_start = "data_period_start"
    data_period_end = "data_period_end"

    initial_frequency = "initial_frequency"
    final_frequency = "final_frequency"
    date = "date"

    # warnings
    # log table
    measure_definition = "measure_definition"
    lookback_period = "lookback_period"
    daily_value = "daily_value"
    condition_value = "condition_value"
    condition = "condition"
    warning_text = "warning_text"
    # for plots
    type_of_qw = "type_of_qw"
    average = "average"
    UCL = "UCL"
    LCL = "LCL"
    title = "title"

    # for grid generation
    geometry = "geometry"
    grid_id = "grid_id"
    elevation = "elevation"
    land_use = "land_use"
    field_name = "field_name"
    type_code = "type_code"
    prior_probability = "prior_probability"

    # device activity statistics
    event_cnt = "event_cnt"
    unique_cell_cnt = "unique_cell_cnt"
    unique_location_cnt = "unique_location_cnt"
    sum_distance_m = "sum_distance_m"
    unique_hour_cnt = "unique_hour_cnt"
    mean_time_gap = "mean_time_gap"
    stdev_time_gap = "stdev_time_gap"

    # signal
    signal_strength = "signal_strength"
    distance_to_cell = "distance_to_cell"
    distance_to_cell_3D = "distance_to_cell_3D"
    joined_geometry = "joined_geometry"
    range = "range"
    path_loss_exponent = "path_loss_exponent"
    azimuth_signal_strength_back_loss = "azimuth_signal_strength_back_loss"
    elevation_signal_strength_back_loss = "elevation_signal_strength_back_loss"

    # for cell footprint
    signal_dominance = "signal_dominance"
    group_id = "group_id"
    cells = "cells"
    group_size = "group_size"

    # time segments
    time_segment_id = "time_segment_id"
    start_timestamp = "start_timestamp"
    end_timestamp = "end_timestamp"
    state = "state"
    is_last = "is_last"

    # for cell connection probability
    cell_connection_probability = "cell_connection_probability"
    posterior_probability = "posterior_probability"

    # dps (daily permanence score)
    dps = "dps"
    time_slot_initial_time = "time_slot_initial_time"
    time_slot_duration = "time_slot_duration"

    # diaries
    stay_type = "stay_type"
    activity_type = "activity_type"
    initial_timestamp = "initial_timestamp"
    final_timestamp = "final_timestamp"
