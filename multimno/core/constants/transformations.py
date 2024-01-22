"""
Data transformations types modukle
"""


class Transformations:
    """
    Class that enumerates the multiple data transformations types.
    """

    converted_timestamp = 1
    other_conversion = 2
    no_transformation = 9

    event_syntactic_cleaning_possible_transformations = [1, 2, 9]
