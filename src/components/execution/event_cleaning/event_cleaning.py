import pyspark.sql.functions as psf

from core.component import Component
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.data_objects.silver.silver_event_data_object import SilverEventDataObject


class EventCleaning(Component):
    COMPONENT_ID = "EventCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)
        self.initalize_data_objects()

    def initalize_data_objects(self):
        # Input
        bronze_event_path = self.config.get(
            "Paths.Bronze", "event_data_bronze")
        bronze_event_do = BronzeEventDataObject(self.spark, bronze_event_path)
        self.input_data_objects = {
            BronzeEventDataObject.ID: bronze_event_do
        }

        # Output
        silver_event_path = self.config.get(
            "Paths.Silver", "event_data_silver")
        silver_event_do = BronzeEventDataObject(self.spark, silver_event_path)
        self.output_data_objects = {
            SilverEventDataObject.ID: silver_event_do
        }

    def transform(self):
        # Get input data objects
        df_events = self.input_data_objects[BronzeEventDataObject.ID].df

        # Filter by cell_id length
        cell_id_length = self.config.getint(
            "Component.EventIngestion", "cell_id_length")
        df_events = df_events.filter(psf.length(
            psf.col('cell_id')) == cell_id_length)

        # Set output data objects
        self.output_data_objects[SilverEventDataObject.ID].df = df_events
