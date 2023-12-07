from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import IntegerType, TimestampType, ArrayType, StructType, StructField
import random
import datetime
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

class SyntheticEvents:
    appname = "synthetic_events_generator"
    seed = 12345
    n_agents = 100
    n_events_per_agent = 1000
    n_partitions = 24
    timestamp_generator_type = "equal_gaps" # "equal_gaps" is 
    timestamp_format = "%Y-%m-%dT%H:%M:%S"
    starting_timestamp = datetime.datetime.strptime("2020-01-01T00:00:00", timestamp_format)
    ending_timestamp = datetime.datetime.strptime("2020-04-01T00:00:00", timestamp_format)
    location_generator_type = "random_cell_id" # also needs support for "lat_lon" generators
    cell_id_min = 1
    cell_id_max = 200
    mcc = 123 # Will we need better mcc generation later?
    output_records_path = "/opt/dev/data/synthetic_events"

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
                    timestamp_generator_params = (self.timestamp_generator_type, self.starting_timestamp, self.ending_timestamp),
                    cell_id_generator_params = (self.location_generator_type, self.cell_id_min, self.cell_id_max, self.seed)
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
        agents_df.printSchema()
        # For each agent, run event generation
        records_df = agents_df.withColumn("record_tuple", explode(generate_agent_records("n_events", "starting_event_id", "timestamp_generator_params", "cell_id_generator_params")))\
            .select(["*", "record_tuple.*"])
        records_df = records_df.select(
            ColNames.user_id,
            ColNames.partition_id,
            ColNames.timestamp,
            ColNames.mcc,
            ColNames.cell_id#,
            #ColNames.latitude,
            #ColNames.longitude
        )

        #TODO create DataObject of records df
        #TODO create config and parse
        #TODO create test file to run this?

        records_df.show(10, False)
        # Write records partitioned by partition_id
        records_df.write.mode("append").partitionBy("partition_id").format("parquet").save(self.output_records_path)
        return None



if __name__ == "__main__":

    # test start
    myconf = {"a":"b"} #...
    test_generator = SyntheticEvents(myconf)
    test_data = test_generator.run()
