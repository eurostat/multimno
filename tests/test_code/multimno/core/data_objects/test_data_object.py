from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from multimno.core.io_interface import ParquetInterface
from tests.test_code.fixtures import spark_session

# Dummy to avoid linting errors using pytest
fixtures = [spark_session]


def test_do_initialization(spark_session):
    do = BronzeEventDataObject(spark_session, "")

    assert isinstance(do, BronzeEventDataObject)
    assert isinstance(do.interface, ParquetInterface)
