class ErrorTypes:
    missing_value = 1
    not_right_syntactic_format = 2
    no_error = 9

    # This shows the possible error types that can happen in syntactic event cleaning
    # This is used for creating the quality metrics data object
    event_syntactic_cleaning_possible_errors = [1, 2, 9]
