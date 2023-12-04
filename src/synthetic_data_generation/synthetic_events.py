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

class SyntheticEvents(Component):
    seed = 12345
    n_agents = 100
    n_events_per_agent = 1000
    starting_timestamp = "2020-01-01T00:00:00"
    ending_timestamp = "2020-04-01T00:00:00"
    location_generator_type = "random_cell_id" # also needs support for "lat_lon" generators
    timestamp_generator_type = "equal_gaps" # "equal_gaps" is 
    mcc = 123 # Will we need better mcc generation later?

    def __init__(config: dict):
        self.config = config

    def generate_records():
        """
        """
        # Init Spark
        # Initialize agents sequentially
        #   Set starting event id, user_id, mcc, location_generator, timestamp_generator, rng seed, number of events
        # Parallelize agents in Spark
        # For each agent, run event generation
        #   Set user_id to agent's user_id
        #   Set timestamp according to timestamp generator

        
        return 3



if __name__ == "__main__":

    # test start
    myconf = {"a":"b"} #...
    test_generator = SyntheticEvents(myconf)
    test_data = test_generator.generate_records()
