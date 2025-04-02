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
    mnc = "mnc"
    plmn = "plmn"
    cell_id = "cell_id"
    latitude = "latitude"
    longitude = "longitude"
    error_flag = "error_flag"
    transformation_flag = "transformation_flag"
    affected_field = "affected_field"
    domain = "domain"
    is_last_event = "is_last_event"  # 0: initial, 1: final

    altitude = "altitude"
    antenna_height = "antenna_height"
    directionality = "directionality"
    azimuth_angle = "azimuth_angle"
    elevation_angle = "elevation_angle"
    horizontal_beam_width = "horizontal_beam_width"
    vertical_beam_width = "vertical_beam_width"
    power = "power"
    range = "range"
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
    field_name = "field_name"
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

    # top frequent errors
    error_value = "error_value"
    error_count = "error_count"
    accumulated_percentage = "accumulated_percentage"

    # for grid generation
    geometry = "geometry"
    grid_id = "grid_id"
    inspire_id = "INSPIRE_id"
    origin = "origin"
    elevation = "elevation"
    land_use = "land_use"
    type_code = "type_code"
    main_landuse_category = "main_landuse_category"
    landuse_area_ratios = "landuse_areas"
    prior_probability = "prior_probability"
    ple_coefficient = "environment_ple_coefficient"
    quadkey = "quadkey"

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
    path_loss_exponent = "path_loss_exponent"
    azimuth_signal_strength_back_loss = "azimuth_signal_strength_back_loss"
    elevation_signal_strength_back_loss = "elevation_signal_strength_back_loss"

    # for cell footprint
    signal_dominance = "signal_dominance"
    group_id = "group_id"
    cells = "cells"
    group_size = "group_size"

    # cell footprint quality metrics
    number_of_events = "number_of_events"
    percentage_total_events = "percentage_total_events"

    # Nearby cells and cell overlap
    overlapping_cell_ids = "overlapping_cell_ids"
    cell_id_a = "cell_id_a"
    cell_id_b = "cell_id_b"
    distance = "distance"

    # time segments
    time_segment_id = "time_segment_id"
    start_timestamp = "start_timestamp"
    end_timestamp = "end_timestamp"
    last_event_timestamp = "last_event_timestamp"
    state = "state"
    is_last = "is_last"

    # for cell connection probability
    cell_connection_probability = "cell_connection_probability"
    posterior_probability = "posterior_probability"

    # dps (daily permanence score)
    dps = "dps"
    stay_duration = "stay_duration"
    time_slot_initial_time = "time_slot_initial_time"
    time_slot_end_time = "time_slot_end_time"
    id_type = "id_type"

    num_unknown_devices = "number_unknown_devices"
    pct_unknown_devices = "percentage_unknown_devices"

    # midterm permanence score
    mps = "mps"
    day_type = "day_type"
    time_interval = "time_interval"
    regularity_mean = "regularity_mean"
    regularity_std = "regularity_std"

    # longterm permanence score
    lps = "lps"
    total_frequency = "total_frequency"
    frequency_mean = "frequency_mean"
    frequency_std = "frequency_std"
    start_date = "start_date"
    end_date = "end_date"
    season = "season"

    # diaries
    stay_type = "stay_type"
    activity_type = "activity_type"
    initial_timestamp = "initial_timestamp"
    final_timestamp = "final_timestamp"

    # present population
    device_count = "device_count"
    population = "population"

    # zone to grid mapping
    zone_id = "zone_id"
    hierarchical_id = "hierarchical_id"
    dataset_id = "dataset_id"

    # for spatial data
    category = "category"
    zone_id = "zone_id"
    level = "level"
    parent_id = "parent_id"
    iso2 = "iso2"
    iso3 = "iso3"
    name = "name"
    dataset_id = "dataset_id"
    hierarchical_id = "hierarchical_id"
    eurostat_code = "eurostat_code"
    timezone = "timezone"

    # for usual environment labels
    label = "label"
    label_rule = "label_rule"

    # for usual environment labeling quality metrics
    labeling_quality_metric = "metric"
    labeling_quality_count = "count"
    labeling_quality_min = "min"
    labeling_quality_max = "max"
    labeling_quality_avg = "avg"

    # for usual environment aggregation
    weighted_device_count = "weighted_device_count"
    tile_weight = "tile_weight"
    device_tile_weight = "device_tile_weight"

    # for tourism stays
    zone_weight = "zone_weight"
    is_overnight = "is_overnight"
    zone_weights_list = "zone_weights_list"
    zone_ids_list = "zone_ids_list"

    # for tourism trips
    trip_id = "trip_id"
    visit_id = "visit_id"
    trip_start_timestamp = "trip_start_timestamp"
    time_segment_ids_list = "time_segment_ids_list"
    is_trip_finished = "is_trip_finished"
    time_period = "time_period"
    country_of_origin = "country_of_origin"
    avg_destinations = "avg_destinations"
    avg_nights_spent_per_destination = "avg_nights_spent_per_destination"
    geography_id = "geography_id"
    nights_spent = "nights_spent"
    num_of_departures = "num_of_departures"
    country_of_destination = "country_of_destination"

    # internal migration
    previous_zone = "previous_zone"
    new_zone = "new_zone"
    migration = "migration"
    start_date_previous = "start_date_previous"
    end_date_previous = "end_date_previous"
    season_previous = "season_previous"
    start_date_new = "start_date_new"
    end_date_new = "end_date_new"
    season_new = "season_new"

    # internal migration quality metrics
    previous_home_users = "previous_home_users"
    new_home_users = "new_home_users"
    common_home_users = "common_home_users"

    # estimation factors
    deduplication_factor = "deduplication_factor"
    mno_to_target_population_factor = "mno_to_target_population_factor"


class SegmentStates:

    STAY = 1
    MOVE = 2
    UNDETERMINED = 3
    UNKNOWN = 4
    ABROAD = 5

    STR_NAMES = {STAY: "stay", MOVE: "move", UNDETERMINED: "undetermined", UNKNOWN: "unknown", ABROAD: "abroad"}

    # Add reverse mapping
    STR_TO_INDEX = {v: k for k, v in STR_NAMES.items()}
