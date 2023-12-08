import pyspark.sql.functions as psf

from core.component import Component
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY

class EventCleaning(Component):
    COMPONENT_ID = "EventCleaning"

    def initalize_data_objects(self):
        # Input
        bronze_event_path = self.config.get(
            CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        bronze_event_do = BronzeEventDataObject(self.spark, bronze_event_path)
        self.input_data_objects = {
            BronzeEventDataObject.ID: bronze_event_do
        }

        # Output
        silver_event_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_data_silver")
        silver_event_do = SilverEventDataObject(self.spark, silver_event_path)
        self.output_data_objects = {
            SilverEventDataObject.ID: silver_event_do
        }

    def read(self):
        do = self.input_data_objects[BronzeEventDataObject.ID]
        sdate = self.config.get(EventCleaning.COMPONENT_ID, 'dates_string')
        path = f"{do.default_path}/{sdate}"
        do.read(path)

    def transform(self):
        # Get input data objects
        df_events = self.input_data_objects[BronzeEventDataObject.ID].df

        # TODO: Complete component
        # Filter by cell_id length
        # cell_id_length = self.config.getint(
        #     self.COMPONENT_ID, "cell_id_length")
        # df_events = df_events.filter(psf.length(
        #     psf.col('cell_id')) == cell_id_length)
        
        # Parse timestamp to timestamp
        df_events = df_events.withColumn('timestamp', psf.to_timestamp('timestamp'))

        # Extract year month day 
        df_events = df_events.withColumns({
            'year': psf.year('timestamp'),
            'month': psf.month('timestamp'),
            'day': psf.dayofmonth('timestamp'),
        })

        # Set output data objects
        self.output_data_objects[SilverEventDataObject.ID].df = df_events
