"""
Transformations Error types module.
"""


class ErrorTypes:
    """
    Class that enumerates the multiple error types of data transformations.
    """

    missing_value = 1
    not_right_syntactic_format = 2
    out_of_admissible_values = 3
    inconsistency_between_variables = 4
    no_location = 5
    out_of_bounding_box = 6
    no_error = 9
    different_location_duplicate = 10
    same_location_duplicate = 11

    # This shows the possible error types that can happen in syntactic event cleaning
    # This is used for creating the quality metrics data object
    event_syntactic_cleaning_possible_errors = [1, 2, 3, 4, 5, 6, 9]

    # This shows the possible error types that can happen in deduplication
    event_deduplication_possible_types = [10, 11]


class NetworkErrorType:
    """
    Class that enumerates the multiple error types present in network topology data.
    """

    NO_ERROR = 0
    NULL_VALUE = 1
    OUT_OF_RANGE = 2
    UNSUPPORTED_TYPE = 3
    CANNOT_PARSE = 4
    INITIAL_ROWS = 100
    FINAL_ROWS = 101


class SemanticErrorType:
    """
    Class that enumerates the multiple error types associated to event semantic checks.
    """

    NO_ERROR = 0
    CELL_ID_NON_EXISTENT = 1
    CELL_ID_NOT_VALID = 2
    INCORRECT_EVENT_LOCATION = 3
    SUSPICIOUS_EVENT_LOCATION = 4
