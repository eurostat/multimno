"""
List of names of the sub-daily periods/time intervals, sub-monthly periods/day types, and sub-yearly/seasons used
in the Permanence Score components.
"""


class PeriodBase:
    @classmethod
    def is_valid_type(cls, value: str) -> bool:
        """
        Check if the given value is a valid type.

        Args:
            value: String to check

        Returns:
            bool: True if value is a valid type, False otherwise
        """
        return value in {
            getattr(cls, attr) for attr in dir(cls) if not attr.startswith("_") and isinstance(getattr(cls, attr), str)
        }

    @classmethod
    def values(cls) -> set[str]:
        """
        Get all string values defined in the class.

        Returns:
            set[str]: Set of all string values
        """
        return {
            getattr(cls, attr) for attr in dir(cls) if not attr.startswith("_") and isinstance(getattr(cls, attr), str)
        }


class TimeIntervals(PeriodBase):
    ALL = "all"
    NIGHT_TIME = "night_time"
    WORKING_HOURS = "working_hours"
    EVENING_TIME = "evening_time"


class DayTypes(PeriodBase):
    ALL = "all"
    WORKDAYS = "workdays"
    HOLIDAYS = "holidays"
    WEEKENDS = "weekends"
    MONDAYS = "mondays"
    TUESDAYS = "tuesdays"
    WEDNESDAYS = "wednesdays"
    THURSDAYS = "thursdays"
    FRIDAYS = "fridays"
    SATURDAYS = "saturdays"
    SUNDAYS = "sundays"

    # mapping of day numbers to their respective days
    WEEKDAY_MAP = {MONDAYS: 0, TUESDAYS: 1, WEDNESDAYS: 2, THURSDAYS: 3, FRIDAYS: 4, SATURDAYS: 5, SUNDAYS: 6}


class Seasons(PeriodBase):
    WINTER = "winter"
    SPRING = "spring"
    SUMMER = "summer"
    AUTUMN = "autumn"
    ALL = "all"


class PeriodCombinations:
    """All possible period combinations as flat lists."""

    # Basic combinations
    ALL_PERIODS = (Seasons.ALL, DayTypes.ALL, TimeIntervals.ALL)
    NIGHT_TIME_ALL = (Seasons.ALL, DayTypes.ALL, TimeIntervals.NIGHT_TIME)
    WORKING_HOURS_WORKDAYS = (Seasons.ALL, DayTypes.WORKDAYS, TimeIntervals.WORKING_HOURS)
