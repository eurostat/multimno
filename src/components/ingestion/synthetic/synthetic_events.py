from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import udf, explode, sha2, col
from pyspark.sql.types import IntegerType, TimestampType, ArrayType, StructType, StructField, BinaryType

import random
import datetime

from core.component import Component
from common.constants.columns import ColNames
""" 
Input parameters:

    number of agents
    number of events per agent
    dates
    Probabilities for generating data in optional fields
    Probabilities for intentional error generation
        Wrong type
        Unbound values
        nullable mandatory fields

Functionalities:

    IMSI pseudo-anonymized 256 bits
    Timestamp format without tz. (Implicit UTC+0 timezone)
    Not relational event information.
        One agent multiple random events
    Generate erroneous data to be cleaned.
        Nullable values in mandatory fields
        Out of bounds dates
        Erroneous data types. (Ex: String in float field)
    Generation of Optional fields
"""

return_type = ArrayType(StructType([StructField(name="event_id", dataType=IntegerType(), nullable=False), 
                                   StructField(name="timestamp", dataType=TimestampType(), nullable=False),
                                   StructField(name="cell_id", dataType=IntegerType(), nullable=False)]))
@udf(returnType=return_type)
def generate_agent_records(n_events, starting_event_id, timestamp_generator_params, cell_id_generator_params):
    """
    UDF to generate records from agent parameters.
    Generates an array of (event_id, timestamp, cell_id) tuples.

    Args:
        n_events (_type_): _description_
        starting_event_id (_type_): _description_
        timestamp_generator_params (_type_): _description_
        cell_id_generator_params (_type_): _description_

    Returns:
        _type_: _description_
    """
    #TODO timestamp generator types
    timestamp_generator_type = timestamp_generator_params[0]
    if (timestamp_generator_type == "equal_gaps"):
        starting_timestamp = timestamp_generator_params[1]
        ending_timestamp = timestamp_generator_params[2]
        gap_length_s = (ending_timestamp - starting_timestamp) / n_events
        current_timestamp = starting_timestamp 
        timestamps = []
        for i in range(n_events):
            timestamps.append(current_timestamp)
            current_timestamp += gap_length_s
    # TODO cell generator types 
    cell_id_generator_type = cell_id_generator_params[0]
    if (cell_id_generator_type == "random_cell_id"):
        cell_id_min = cell_id_generator_params[1]
        cell_id_max = cell_id_generator_params[2]
        #TODO might want to add user_id to random seed, otherwise the cell ids are identical for all users 
        random.seed(cell_id_generator_params[3]) #Is this independent enough from the other parallel randoms to ensure the same results each run? 
        cell_ids = [random.randint(cell_id_min, cell_id_max) for i in range(n_events)]
    event_ids = [i for i in range(starting_event_id, starting_event_id + n_events)]
    events = zip(event_ids, timestamps, cell_ids)
    return events

class SyntheticEvents(Component):
    COMPONENT_ID = "SyntheticEvents"

    supported_timestamp_generator_types = ["equal_gaps"]
    supported_location_generator_types = ["random_cell_id"]

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)
        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        self.n_agents = self.config.getint(self.COMPONENT_ID, "n_agents")
        self.n_events_per_agent = self.config.getint(self.COMPONENT_ID, "n_events_per_agent")
        self.n_partitions = self.config.getint(self.COMPONENT_ID, "n_partitions")

        self.timestamp_generator_type = self.config.get(self.COMPONENT_ID, "timestamp_generator_type") #TODO support for other timestamp generation methods
        if self.timestamp_generator_type not in self.supported_timestamp_generator_types:
            raise ValueError(f"Unsupported timestamp_generator_type: {self.timestamp_generator_type}. Supported types are: {self.supported_timestamp_generator_types}")
        timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")
        self.starting_timestamp = datetime.datetime.strptime(self.config.get(self.COMPONENT_ID, "starting_timestamp"), timestamp_format) 
        self.ending_timestamp = datetime.datetime.strptime(self.config.get(self.COMPONENT_ID, "ending_timestamp"), timestamp_format)
        
        self.location_generator_type = self.config.get(self.COMPONENT_ID, "location_generator_type") # TODO support for "lat_lon" generator, other cell_id based generator 
        if self.location_generator_type not in self.supported_location_generator_types:
            raise ValueError(f"Unsupported location_generator_type: {self.location_generator_type}. Supported types are: {self.supported_location_generator_types}")
        self.cell_id_min = self.config.getint(self.COMPONENT_ID, "cell_id_min")
        self.cell_id_max = self.config.getint(self.COMPONENT_ID, "cell_id_max")
        
        self.mcc = self.config.getint(self.COMPONENT_ID, "mcc") # Will we need better mcc generation later? 

    def initalize_data_objects(self):
    
        output_records_path = self.config.get(self.COMPONENT_ID, "output_records_path")

        # TODO csv interface support needed ?
        bronze_event = BronzeEventDataObject(self.spark, output_records_path, 
                                             #partition_columns = [ColNames.year, ColNames.month, ColNames.day]
                                            ) # ParquetInterface()

        self.output_data_objects = {
            "SyntheticEvents": bronze_event
        }

    def read(self):
        pass # No input datasets are used in this component

    def transform(self):
        spark = self.spark
    
        # Initialize each agent, generate Spark dataframe
        agents = self.generate_agents()
        agents_df = spark.createDataFrame(agents)
        # Generate events for each agent. Since the UDF generates a list, it has to be exploded to separate the rows.
        records_df = agents_df.withColumn("record_tuple", explode(generate_agent_records("n_events", "starting_event_id", "timestamp_generator_params", "cell_id_generator_params")))\
            .select(["*", "record_tuple.*"])
        records_df = self.calc_hashed_user_id(records_df)

        records_df = records_df.withColumn(ColNames.timestamp, col(ColNames.timestamp).cast("string"))
        records_df = records_df.withColumn(ColNames.cell_id, col(ColNames.cell_id).cast("string"))
        records_df = records_df.withColumn(ColNames.mcc, col(ColNames.mcc).cast(IntegerType()))

        #TODO use DataObject schema for selecting the columns?
        bronze_columns = [i.name for i in BronzeEventDataObject.SCHEMA]
        for unsupported_column in ["longitude", "latitude", "loc_error"]: # TODO integrate with ColNames
            bronze_columns.remove(unsupported_column)

        records_df = records_df.select(
            bronze_columns
        )

        #records_df = records_df.withColumn(ColNames.year, year(col(ColNames.timestamp)))
        #records_df = records_df.withColumn(ColNames.month, month(col(ColNames.timestamp)))
        #records_df = records_df.withColumn(ColNames.day, dayofmonth(col(ColNames.timestamp)))
        
        # Assign output data object dataframe
        self.output_data_objects["SyntheticEvents"].df = records_df

    def write(self):
        super().write()

        # self.output_data_objects["SyntheticEvents"].write(partition_columns="partition_id")

    def execute(self):
        # super().execute()
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.transform()
        self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def generate_agents(self) -> []:
        """
        Generate agent rows according to parameters.
        Each agent should include the information needed to generate the records for that user.
        """
        # Initialize agents sequentially
        #TODO event ids should be numbered per partition, not global?
        agents = []
        starting_event_id = 0
        for user_id in range(self.n_agents):
            partition_id = user_id % self.n_partitions
            agents.append(
                Row(
                    user_id=user_id, 
                    partition_id=partition_id, 
                    starting_event_id=starting_event_id, 
                    mcc=self.mcc, 
                    n_events=self.n_events_per_agent,
                    timestamp_generator_params = (self.timestamp_generator_type, self.starting_timestamp, self.ending_timestamp),
                    cell_id_generator_params = (self.location_generator_type, self.cell_id_min, self.cell_id_max, self.seed)
                )
            )
            starting_event_id += self.n_events_per_agent
        return agents
    

    def calc_hashed_user_id(self, df) -> DataFrame:
        """
        Calculates SHA2 hash of user id, takes the first 31 bits and converts them to a non-negative 32-bit integer.
        """
        df = df.withColumn("ms_id_binary", col(ColNames.user_id).cast(BinaryType()))
        
        df = df.withColumn(ColNames.user_id,
                          sha2(col("ms_id_binary"), numBits=256))

        df = df.drop("ms_id_binary")

        return df



if __name__ == "__main__":
    #TODO Remove code execution from here. Implement in notebook and/or test.
    # test start
    root_path = "/opt/dev"
    general_config = f"{root_path}/pipe_configs/configurations/general_config.ini"
    component_config = f"{root_path}/pipe_configs/configurations/synthetic_events/synthetic_events.ini"
    test_generator = SyntheticEvents(general_config, component_config)

    test_generator.execute()
