from core.component import Component
from core.data_objects.landing.landing_event_data_object import LandingEventDataObject
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.settings import CONFIG_LANDING_PATHS_KEY
from core.settings import CONFIG_BRONZE_PATHS_KEY

class EventCleaning(Component):
    COMPONENT_ID = "EventCleaning"

    def initalize_data_objects(self):
        # Input
        landing_event_path = self.config.get(
            CONFIG_LANDING_PATHS_KEY, "event_data_landing")
        landing_event_do = LandingEventDataObject(self.spark, landing_event_path)
        self.input_data_objects = {
            LandingEventDataObject.ID: landing_event_do
        }

        # Output
        bronze_event_path = self.config.get(
            CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        bronze_event_do = BronzeEventDataObject(self.spark, bronze_event_path)
        self.output_data_objects = {
            BronzeEventDataObject.ID: bronze_event_do
        }

    def read(self):
        self.logger.info(f"Read method {self.COMPONENT_ID}")
        # do = self.input_data_objects[LandingEventDataObject.ID]
        # # do.read()
        # # do.df.show()

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        spark = self.spark
        schema = self.output_data_objects[BronzeEventDataObject.ID].SCHEMA


        timestamp_format = self.config.get(EventCleaning.COMPONENT_ID, 'timestamp_format')
        input_timezone = self.config.get(EventCleaning.COMPONENT_ID, 'input_timezone')
        data_period_start = self.config.get(EventCleaning.COMPONENT_ID, 'data_period_start')
        data_period_end = self.config.get(EventCleaning.COMPONENT_ID, 'data_period_end')
        do_bounding_box_filtering = self.config.getboolean(EventCleaning.COMPONENT_ID, 'do_bounding_box_filtering', fallback=False)
        bounding_box = self.config.geteval(EventCleaning.COMPONENT_ID, 'bounding_box')
        mandatory_columns_casting_dict = self.config.geteval(EventCleaning.COMPONENT_ID, 'mandatory_columns_casting_dict')
        optional_columns_casting_dict = self.config.geteval(EventCleaning.COMPONENT_ID, 'optional_columns_casting_dict')


        pass

    def write(self):
        self.logger.info(f"Write method {self.COMPONENT_ID}")
        pass

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        self.transform()
        self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")