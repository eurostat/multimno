from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
import random

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

@udf#(returnType=IntegerType())
def generate_agent_records(n_events, starting_event_id, timestamp_generator_params, cell_id_generator_params):
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
        random = random.seed(cell_id_generator_params[3])
        cell_ids = []
        for i in range(n_events):
            cell_ids.append(random.randint(cell_id_min, cell_id_max))
    event_ids = [range(starting_event_id, starting_event_id + n_events)]
    events = zip(event_ids, timestamps, cell_ids)
    return events

class SyntheticEvents:
    appname = "synthetic_events_generator"
    seed = 12345
    n_agents = 100
    n_events_per_agent = 1000
    n_partitions = 24
    timestamp_generator_type = "equal_gaps" # "equal_gaps" is 
    starting_timestamp = "2020-01-01T00:00:00"
    ending_timestamp = "2020-04-01T00:00:00"
    location_generator_type = "random_cell_id" # also needs support for "lat_lon" generators
    cell_id_min = 1
    cell_id_max = 200
    mcc = 123 # Will we need better mcc generation later?

    def __init__(self, config: dict):
        self.config = config

    def start_spark(self):
        spark = SparkSession.builder\
            .config("spark.driver.host", "localhost")\
            .config("spark.eventLog.enabled", "true")\
            .config("spark.eventLog.dir", "/opt/spark/spark-events")\
            .config("spark.history.fs.logDirectory", "/opt/spark/spark-events")\
            .appName(self.appname).getOrCreate()
        return spark

    def generate_agents(self) -> []:
        """
        Generate agents according to parameters. 
        Each agent should contain the information needed to generate the specified number of records for that user.
        """
        # Initialize agents sequentially
        #TODO event ids should be numbered per partition, not global?
        agents = []
        starting_event_id = 0
        for i in range(self.n_agents):
            user_id = i
            partition_id = user_id % self.n_partitions
            agents.append(
                Row(
                    user_id=user_id, 
                    partition_id=partition_id, 
                    starting_event_id=starting_event_id, 
                    mcc=self.mcc, 
                    n_events=self.n_events_per_agent,
                    starting_timestamp = self.starting_timestamp,
                    ending_timestamp = self.ending_timestamp,
                    timestamp_generator_params = (self.timestamp_generator_type, self.starting_timestamp, self.ending_timestamp),
                    cell_id_generator_params = (self.location_generator_type, self.cell_id_min, self.cell_id_max)
                )
            )
            starting_event_id += self.n_events_per_agent
        return agents
    


    def run(self):
        """
        """
        # Init Spark
        spark = self.start_spark()
        agents = self.generate_agents()
        print(agents)
        # Parallelize agents in Spark
        agents_df = spark.createDataFrame(agents)
        agents_df.show(10)
        # For each agent, run event generation
        agents_df.select(generate_agent_records("n_events", "starting_event_id", "timestamp_generator_params", "cell_id_generator_params").alias("Wadup")).show(10)
        #   Set user_id to agent's user_id
        #   Set timestamp according to timestamp generator
        # Write records partitioned by partition_id

        return None



if __name__ == "__main__":

    # test start
    myconf = {"a":"b"} #...
    test_generator = SyntheticEvents(myconf)
    test_data = test_generator.run()
