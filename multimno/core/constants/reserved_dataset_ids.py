"""
Reserved zoning dataset IDs. At this moment they refer to the INSPIRE 100m and 1km grids, which receive a special
treatment as their geometries are not needed in order to map the reference grid tiles (the INSPIRE 100m grid) to
them.
"""


class ReservedDatasetIDs:
    """
    Class that enumerates reserved dataset IDs.
    """

    INSPIRE_1km = "INSPIRE_1km"
    INSPIRE_100m = "INSPIRE_100m"

    def __contains__(self, value):
        if self.INSPIRE_100m == value:
            return True
        if self.INSPIRE_1km == value:
            return True
        return False
