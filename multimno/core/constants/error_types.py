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

    # This shows the possible error types that can happen in syntactic event cleaning
    # This is used for creating the quality metrics data object
    event_syntactic_cleaning_possible_errors = [1, 2, 3, 4, 5, 6, 9]