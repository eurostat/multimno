import random

from core.component import Component
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject


class EventSynthetic(Component):
    COMPONENT_ID = "EventSynthetic"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

    def initalize_data_objects(self):
        # Output
        bronze_event_path = self.config.get(
            "Paths.Bronze", "event_data_bronze")
        bronze_event = BronzeEventDataObject(self.spark, bronze_event_path)
        self.output_data_objects = {
            BronzeEventDataObject.ID: bronze_event
        }

    def read(self):
        pass

    def transform(self):
        self.logger.info("Starting Synthetic data generation")
        # Initialize empty dataframe
        # MVP - Synthetic
        spark = self.spark
        schema = self.output_data_objects[BronzeEventDataObject.ID].SCHEMA

        n_agents = self.config.getint('Component.EventSynthetic', 'n_agents')
        n_events = self.config.getint('Component.EventSynthetic', 'n_events')
        seed = self.config.getint('Component.EventSynthetic', 'seed')

        sdate = self.config.getint('Component.EventSynthetic', 'dates_string')

        data = []
        for _ in range(n_agents):

            # Empty dataframe
            # df = spark.sparkContext.emptyRDD().toDF(schema)

            # Random user_id
            # random.seed(seed)
            user_id = random.randbytes(32)
            self.logger.debug(f"User generated: {user_id}")

            for _ in range(n_events):
                # Random timestamp in path
                random_date = f"{sdate}T{random.randint(0, 23):02}:{random.randint(0, 59):02}:{random.randint(0, 59):02}"
                # random mcc
                random_mcc = random.randint(0, 999)
                # random cell_id
                cell_id = f"{random_mcc:03}" + \
                    "".join([str(random.randint(0, 9)) for _ in range(12)])

                # latitude
                latitude = None
                # longitude
                longitude = None
                # loc_error
                loc_error = None
                # Create row
                row = [user_id, random_date, random_mcc,
                       cell_id, latitude, longitude, loc_error]
                # Append row to data
                data.append(row)

        df_events = spark.createDataFrame(data, schema)
        # Set output data objects
        self.output_data_objects[BronzeEventDataObject.ID].df = df_events

    def write(self):
        event_do = self.output_data_objects[BronzeEventDataObject.ID]
        sdate = self.config.get('Component.EventSynthetic', 'dates_string')
        path = f"{event_do.interface.path}/{sdate}"
        event_do.write(path)
        self.logger.info("Synthetic data generation finished!")
