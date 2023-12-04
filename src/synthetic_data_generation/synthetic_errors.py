    """
    Module responsible for adding synthetic errors to existing synthetic event data.
    """

class SyntheticErrorGenerator:
    input_path = None
    appname = "Synthetic Error Generator"

    def __init__(self, config: dict):
        self.config = config

    def generate_errors():
        # Init Spark
        spark = create_spark_session(config_dict, appname)   
        # Read input events to Spark df
        input_dataobject = DataObject(config[""])
        input_df = DataObject.read_data_object()
        # Get number of events in df (count or rely on config?)
        # For each error type specified in config:
        #   Find number of error events (from minimum count and error-specific probability value)
        #   Generate ids of error records
        #   For each error id, edit the corresponding record
        # Write results    


    # Error types:
    #   user_id is null
    #   user_id is not parsable
    #   cell_id is null
    #   cell_id is not parsable
    #   timestamp is null
    #   timestamp is not parsable
    #   timestamp is not within acceptable bounds
    #   timestamp has/lacks timezone 
