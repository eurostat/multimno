"""
Module responsible for adding synthetic errors to existing synthetic event data.
"""

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StructField, LongType, ArrayType, TimestampType, BooleanType, IntegerType, \
    StringType

# TODO as common function
def create_spark_session(config_dict, appname: str):
    spark_config_dict = config_dict
    spark = SparkSession.builder.appName(appname)
    for key, value in spark_config_dict.items():
        spark = spark.config(key, value)

    session = spark.getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")

    if spark_config_dict.get("spark.checkpoint.dir"):
        session.sparkContext.setCheckpointDir(
            spark_config_dict.get("spark.checkpoint.dir"))

    return session

    
class SyntheticErrorGenerator:
    """
    Class for synthetic error generation based on synthetic data.

    """
    
    def __init__(self, config: dict):
        self.config = config

    
    def read_synthetic_events(self) -> pyspark.sql.DataFrame:
        """
        Reads synthetically generated events.

        Returns:
            pyspark.sql.DataFrame: clean synthetic events
        """
        
        spark = create_spark_session(self.config["spark"], "Synthetic Error Generator")  

        if self.config["synthetic_events_format"] == "parquet":
            df = spark.read.parquet(self.config["synthetic_events_folder_path"])
        if self.config["synthetic_events_format"] == "csv":
            df = spark.read.option("delimiter", ";").option("header", True).option("inferSchema", True).csv(self.config["synthetic_events_folder_path"])

        return df
      
    def generate_nulls_in_mandatory_fields(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Generates null values in mandatory field columns based on probabilities from config.

        Args:
            df (pyspark.sql.DataFrame): clean synthetic data

        Returns:
            pyspark.sql.DataFrame: synthetic records dataframe with nulls in some rows
        """
        
        # Reading in two probability parameters from config:
        # First one describes how many rows to create null values for
        # Second one selects the maximum ratio of columns that are allowed to be nulls for rows, that are selected for error generation
        # If 1.0, it means that all mandatory columns can be allowed to be null-s for rows that are selected as including nulls

        null_row_prob = self.config["null_row_probability"]
        max_ratio_of_mandatory_columns = self.config["max_ratio_of_mandatory_columns_to_generate_as_null"]
        seed_param = 999 if self.config["use_fixed_seed"] else None
        mandatory_columns = self.config["mandatory_columns"]

        # 1) sample rows 2) sample columns 3) do a final join/union 
        # Sampling a new dataframe
        df = df.withColumn("user_id_copy", F.col("user"))
        sampled_df = df.sample(null_row_prob, seed = seed_param)
        df_with_nulls = sampled_df

        # Selecting columns based on ratio param
        for column in mandatory_columns:
            df_with_nulls = df_with_nulls.withColumn(column, F.when(F.rand(seed_param) < max_ratio_of_mandatory_columns, 
                                                              F.lit(None)).otherwise(F.col(column)))

        result_df = df\
            .join(df_with_nulls, on = ["user_id_copy", "event_id"], how = "leftanti")\
                [[df.columns]]\
                .unionAll(df_with_nulls)\
                .drop(F.col("user_id_copy"))
        
        # TODO
        # optional sorting here, in case of visual overview based on original ordering? 
        # .orderBy([F.col("user_id_copy"), F.col("event_id")])

        return result_df


    def generate_out_of_bounds_dates(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """

        Args:
            df (pyspark.sql.DataFrame): Data of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Data where some timestamp column values are out of bounds as per config.
        """
        
        out_of_bounds_prob = self.config["out_of_bounds_prob"]
        date_bounds = self.config["date_bounds"]
        seed_param = 999 if self.config["use_fixed_seed"] else None
        events_span_in_months = pd.Timestamp(date_bounds[1]).month - pd.Timestamp(date_bounds[0]).month

        # Current idea is to 1) sample rows 2) sample columns 3) do a final join/union 4) sort
        df = df.withColumn("user_id_copy", F.col("user"))
        df_not_null_dates = df.where(F.col("timestamp").isNotNull()) 

        # TODO this should account for wrong type as well
        # Current approach means that this should be run after nulls, but before wrong type generation

        df_with_sample_column = df_not_null_dates.join(
            df_not_null_dates[["event_id", "user"]].sample(out_of_bounds_prob, seed = seed_param).withColumn("out_of_bounds", F.lit(True)),
            on = ["user", "event_id"],
            how = "left"
        ).withColumn("out_of_bounds", F.when(
            F.col("out_of_bounds").isNull(), F.lit(False)).otherwise(F.col("out_of_bounds")))

        df_with_sample_column = df_with_sample_column.withColumn("months_to_add", 
                                                                 (F.lit(1) + F.randn(seed_param))*F.lit(events_span_in_months))

        df_with_sample_column = df_with_sample_column.withColumn("timestamp",
                                           F.when(F.col("out_of_bounds"), 
                                                  F.add_months(F.col("timestamp"), F.col("months_to_add"))
                                           ).otherwise(F.col("timestamp"))
        ).drop(F.col("months_to_add"))#\
            #.drop(F.col("out_of_bounds"))

        result_df = df\
            .where(F.col("timestamp").isNull())\
            .withColumn("out_of_bounds", F.lit(None))\
                [df.columns + ["out_of_bounds"]]\
        .unionAll(df_with_sample_column[df.columns + ["out_of_bounds"]])\
        .drop(F.col("user_id_copy"))

        # TODO
        # optional sorting here, in case of visual overview based on original ordering? 
        # .orderBy([F.col("user_id_copy"), F.col("event_id")])

        result_df.dropna().show()

        return result_df

    def cast_and_mutate_row(self, df: pyspark.sql.DataFrame, column_name: str, to_type: str, to_value: any) -> pyspark.sql.DataFrame:
        """
        Casts a column from some types to another for a particular row.
        Adds selected value for that row. 

        Args:
            df (pyspark.sql.DataFrame): dataframe to add erronous rows to
            column_name (str): column to change
            from_types (list): list of types to convert from
            to_type (str): type to convert to
            to_value (any): pyspark sql function statement

        Returns:
            pyspark.sql.DataFrame: dataframe with casted and changed rows
        """

        df = df\
            .withColumn(column_name, F.when(
                F.col("error_row"),
                    F.col(column_name).cast(to_type))
                        .otherwise(F.col(column_name))
            )\
            .withColumn(column_name, F.when(
                F.col("error_row"),
                    F.lit(to_value))
                        .otherwise(F.col(column_name))
            )

        return df
    

    def generate_erroneous_type_values(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Casts certain rows to different type and generates values for them.
        Does nothing when output format is parquet.

        Args:
            df (pyspark.sql.DataFrame): dataframe that may have out of bound and null records.

        Returns:
            pyspark.sql.DataFrame: dataframe with type errors.
        """
        
        # Format check
        if self.config["synthetic_events_format"] == "parquet":
            raise ValueError("Cannot generate erronous values for parquet format.")
    
        error_prob = self.config["data_type_error_probability"]
        mandatory_columns = self.config["mandatory_columns"]    
        seed_param = 999 if self.config["use_fixed_seed"] else None

        # Current idea is to 1) select not null rows 2) create a sampling column 3) perform type mutation
        # This creates wrong type values on same selected rows for all mandatory columns
        # Could be improved to select non-nulls for only that specific column, and do sampling for each column seperately
        # Or to sample the probabilities for wrong types seperately?

        # Errors shouldn't be generated, where there are out of bounds records, otherwise the probabilities don't realise in the final data
        # as expected (for instance, the ratio of error records may be much smaller than the probability assigned,
        # because some nulls are re-done as errors)

        # In case the pipeline is such that out of bounds comes later
        if "out_of_bounds" not in df.columns:
            df = df.withColumn("out_of_bounds", F.lit(False))

        df_not_null = df.dropna().filter(F.col("out_of_bounds") == F.lit(False))
        df = df.drop(F.col("out_of_bounds"))

        df_with_sample_column = df_not_null.join(
            df_not_null[["event_id", "user"]].sample(error_prob, seed = seed_param).withColumn("error_row", F.lit(True)),
            on = ["user", "event_id"],
            how = "left"
        )

        df_with_sample_column.show()

        # Iterate over mandatory columns to mutate the type for a sampled row
        # First cast sampled rows, then fill with values
        # TODO refactor more compactly

        for column in mandatory_columns:
            col_dtype = [dtype for name, dtype in df_with_sample_column.dtypes if name == column][0]
            
            if col_dtype in ["string", "str"]:
                to_type = "float",
                to_value = F.lit(F.rand(seed_param))
            
            if col_dtype in ["int", "float"]:
                to_type = "string"
                to_value =  F.lit("error_row")
            
            if col_dtype in ["date", "timestamp"]:
                to_type = "string"
                to_value =  F.lit("random_str")
            
            df_with_sample_column = self.cast_and_mutate_row(
                df_with_sample_column,
                column, 
                to_type,
                to_value
            )
        
        # TODO check a more optional join
        result_df = df.join(df_with_sample_column, on = ["user", "event_id"], how = "leftanti")\
            [df.columns]\
            .unionAll(df_with_sample_column)\
            [df.columns]
        
        return result_df


    def generate_optional_column(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Generates an optional column with a given probability.
        Generates erronous values in optional column.

        Args:
            df (pyspark.sql.DataFrame): dataframe with null, out of bound and erronous records

        Returns:
            pyspark.sql.DataFrame: dataframe with optional column.
        """
        optional_col_probability = self.config["optional_col_probability"]
        use_fixed_seed = self.config["use_fixed_seed"]
        optional_columns = self.config["optional_columns"]    


        pass


    def write_erroneous_records(self, df: pyspark.sql.DataFrame) -> None:
        """
        Writes records with errors to desired format

        Args:
            df (pyspark.sql.DataFrame): dataframe, that has been processed by all (selected) error generating functions
        """

        output_dir = self.config["synthetic_erroneous_records_path"]
        partition_columns = self.config["partition_columns"]
        synthetic_events_format = self.config["synthetic_events_format"]

        if synthetic_events_format == "parquet":
            df.write.mode("overwrite").partitionBy(partition_columns).parquet(output_dir)
        if synthetic_events_format == "csv":
            df.write.option("header", "true").mode("overwrite").format("csv").save(output_dir)


if __name__ == "__main__":

    # Error types:
    #   user_id is null
    #   user_id is not parsable
    #   cell_id is null
    #   cell_id is not parsable
    #   timestamp is null
    #   timestamp is not parsable
    #   timestamp is not within acceptable bounds
    #   timestamp has/lacks timezone 
    
    generator_config = {"null_row_probability":0.2, 
            "data_type_error_probability": 0.2,
            "out_of_bounds_prob": 0.2,
              "max_ratio_of_mandatory_columns_to_generate_as_null": 0.5,
              "use_fixed_seed": True,
              "synthetic_events_folder_path": "/opt/dev/sample_data/synth_modified",
              "synthetic_erroneous_records_path":"/opt/dev/sample_data/synth_errors",
              "mandatory_columns": ["user", "timestamp", "cellid"],
              "partition_columns": ["user"],
              "spark": {"spark.driver.host": "localhost",
                        "spark.driver.memory": "4g",
                        "spark.driver.cores": "4",
                        "spark.executor.memory": "4g",
                        "spark.eventLog.enabled": "true",
                        "spark.eventLog.dir": "/opt/spark/spark-events",
                        "spark.history.fs.logDirectory": "/opt/spark/spark-events",
                        },
                         "synthetic_events_format": "csv" ,
                         "date_bounds": ["2021-01-01", "2021-05-01"]} #...
    
    test_generator = SyntheticErrorGenerator(generator_config)
    synth_data = test_generator.read_synthetic_events()
    synth_data_w_nulls = test_generator.generate_nulls_in_mandatory_fields(synth_data)
    synth_data_w_out_of_bounds_and_nulls = test_generator.generate_out_of_bounds_dates(synth_data_w_nulls) 
    synth_data_w_out_of_bounds_nulls_errors = test_generator.generate_erroneous_type_values(synth_data_w_out_of_bounds_and_nulls) 
    
    # write results
    test_generator.write_erroneous_records(synth_data_w_out_of_bounds_nulls_errors)
    
    synth_data_w_out_of_bounds_nulls_errors\
        .write.option("header", "true").mode("overwrite").format("csv").save("/opt/dev/sample_data/synth_w_nulls/synth_events")
    
