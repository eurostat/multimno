from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.io_interface import ParquetInterface
from tests.test_code.fixtures import spark_fixture

def test_do_initialization(spark_fixture):
    do = BronzeEventDataObject(spark_fixture)
    
    assert isinstance(do, BronzeEventDataObject)
    assert isinstance(do.interface, ParquetInterface)
