"""
Transformations Error types module.
"""


class ErrorTypes:
    """
    Class that enumerates the multiple error types of data transformations.
    """

    NO_ERROR = 0
    NULL_VALUE = 1
    OUT_OF_RANGE = 2
    UNSUPPORTED_TYPE = 3
    CANNOT_PARSE = 4
    NO_MNO_INFO = 5
    NO_LOCATION_INFO = 6
    DUPLICATED = 7

    # This shows the possible error types that can happen in syntactic event cleaning
    # This is used for creating the quality metrics data object
    event_syntactic_cleaning_possible_errors = [0, 1, 2, 3, 4, 5, 6, 7]


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
    DIFFERENT_LOCATION_DUPLICATE = 5


class UeGridIdType:

    UNKNOWN = -99
    DEVICE_OBSERVATION = -1
    UKNOWN_STR = "unknown"
    DEVICE_OBSERVATION_STR = "device_observation"
    GRID_STR = "grid"
