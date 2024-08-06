"""

"""

from pyspark.sql import SparkSession

from multimno.core.data_objects.data_object import PathDataObject
from multimno.core.io_interface import HttpGeoJsonInterface


class LandingHttpGeoJsonDataObject(PathDataObject):
    """
    Class that models input geospatial data in geojson format.
    """

    ID = "LandingGeoJsonDO"

    def __init__(self, spark: SparkSession, url: str, timeout: int, max_retries: int) -> None:

        super().__init__(spark, url)
        self.interface: HttpGeoJsonInterface = HttpGeoJsonInterface()
        self.default_path = url
        self.timeout = timeout
        self.max_retries = max_retries

    def read(self):

        self.df = self.interface.read_from_interface(self.spark, self.default_path, self.timeout, self.max_retries)
