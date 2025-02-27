"""
Reserved zoning dataset IDs. At this moment they refer to the INSPIRE 100m and 1km grids, which receive a special
treatment as their geometries are not needed in order to map the reference grid tiles (the INSPIRE 100m grid) to
them. Also contains the ABROAD tag for marking outbound data. 
"""


class ReservedDatasetIDs:
    """
    Class that enumerates reserved dataset IDs.
    """

    INSPIRE_1km = "INSPIRE_1km"
    INSPIRE_100m = "INSPIRE_100m"
    ABROAD = "ABROAD"  # Currently only used to mark outbound data (which also does not have a grid).

    def __contains__(self, value):
        if self.INSPIRE_100m == value:
            return True
        if self.INSPIRE_1km == value:
            return True
        return False

    @classmethod
    def get_resolution_m(cls, value):
        if cls.INSPIRE_100m == value:
            return 100
        if cls.INSPIRE_1km == value:
            return 1000
        return None
