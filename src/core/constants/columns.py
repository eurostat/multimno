# Reusable internal column names. Useful for referring to the the same column across multiple components.

class ColNames:
    user_id = "user_id"
    partition_id = "partition_id"
    timestamp = "timestamp"
    mcc = "mcc"
    cell_id = "cell_id"
    latitude = "latitude"
    longitude = "longitude"
    loc_error = "loc_error"
    event_id = "event_id"

    year = "year"
    month = "month"
    day = "day"

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

