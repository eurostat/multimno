"""
Module responsible for adding synthetic errors to existing synthetic event data.
"""

import random
import string

import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window

import datetime
import pandas as pd
from core.component import Component
from common.constants.columns import ColNames
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.io_interface import ParquetInterface, CsvInterface
from pyspark.sql.types import StringType, IntegerType, FloatType, BinaryType

    
class SyntheticErrors(Component):
    COMPONENT_ID = "SyntheticErrorsGenerator"
    
    """
    Class for synthetic error generation based on synthetic data.

    """
    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)
        self.timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")
        self.error_prob = self.config.getfloat(self.COMPONENT_ID, 
                                                           "data_type_error_probability") 
        self.max_ratio_of_mandatory_columns = self.config.getfloat(self.COMPONENT_ID, 
                                                           "max_ratio_of_mandatory_columns_to_generate_as_null") 
        
        self.null_row_prob =  self.config.getfloat(self.COMPONENT_ID, 
                                                           "null_row_probability")  
        
        self.out_of_bounds_prob =  self.config.getfloat(self.COMPONENT_ID, 
                                                           "out_of_bounds_probability")  
        
        self.optional_columns = ["loc_error"] #static definition at the moment  #self.config.get(self.COMPONENT_ID, "optional_columns")

        bronze_columns = [i.name for i in BronzeEventDataObject.SCHEMA]

        for optional_column in self.optional_columns:
            bronze_columns.remove(optional_column)
        
        self.unsupported_columns = ["longitude", "latitude"]

        for unsupported_column in self.unsupported_columns: 
            # TODO integrate with ColNames
            # TODO support for lon, lat
            bronze_columns.remove(unsupported_column)

        self.mandatory_columns = bronze_columns
        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        
        self.date_bounds = [
            datetime.datetime.strptime(self.config.get(self.COMPONENT_ID, "starting_timestamp"), self.timestamp_format).date(),
            datetime.datetime.strptime(self.config.get(self.COMPONENT_ID, "ending_timestamp"), self.timestamp_format).date()
        ]
        self.sort_output = self.config.getboolean(self.COMPONENT_ID, "sort_output")
        

    def initalize_data_objects(self):

        self.error_generator_output_path = self.config.get(self.COMPONENT_ID, "error_generator_output_path")
        self.error_generator_input_path = self.config.get(self.COMPONENT_ID, "error_generator_input_path")

        self.output_file_format = self.config.get(self.COMPONENT_ID, "output_file_format")
        
        if self.output_file_format == "parquet" and self.data_type_error_probability != 0:
            raise ValueError("Cannot generate erronous (data mismatch) values for parquet format.")

        if self.output_file_format == "csv":
            self.output_interface = CsvInterface()
        elif self.output_file_format == "parquet":
            self.output_interface = ParquetInterface()
        else:
            raise ValueError("Invalid output format for synthetic errors.")
        
        #init output object: bronze synthetic events
        input_bronze_event = BronzeEventDataObject(self.spark, self.error_generator_input_path, interface = ParquetInterface())

        self.input_data_objects = {
            "SyntheticEvents": input_bronze_event
        }
        
        output_bronze_event = BronzeEventDataObject(self.spark, self.error_generator_output_path , interface = self.output_interface)
        self.output_data_objects = {
            "SyntheticErrors": output_bronze_event
        }
        

    def read(self):
        return super().read()

    def transform(self):
        # spark = self.spark
        
        # Read in clean records and proceed with transformations
        synth_df = self.input_data_objects["SyntheticEvents"].df
        synth_df = synth_df[[self.mandatory_columns]] # TODO optional column support


        # Create a copy of original MSID column for final sorting
        synth_df = synth_df.withColumn("user_id_copy", F.col(ColNames.user_id))

        # Create event id column if missing
        if ColNames.event_id not in synth_df.columns:
            windowSpec = Window.partitionBy(F.col(ColNames.user_id)).orderBy(F.col(ColNames.timestamp))
            synth_df = synth_df.withColumn(ColNames.event_id, F.row_number().over(windowSpec))

        synth_df_w_nulls = self.generate_nulls_in_mandatory_fields(synth_df)
        synth_df_w_out_of_bounds_and_nulls = self.generate_out_of_bounds_dates(synth_df_w_nulls) 
        synth_df_w_out_of_bounds_nulls_errors = self.generate_erroneous_type_values(synth_df_w_out_of_bounds_and_nulls) 
        
        # Sort
        if self.sort_output:
            synth_df_w_out_of_bounds_nulls_errors = synth_df_w_out_of_bounds_nulls_errors.orderBy(["user_id_copy", ColNames.event_id])
        
        error_df = synth_df_w_out_of_bounds_nulls_errors\
            .drop(F.col("user_id_copy"))\
            .drop(F.col(ColNames.event_id))
        
        for col in self.unsupported_columns:
            error_df = error_df.withColumn(col, F.lit(None).cast(StringType()))

        for col in self.optional_columns:
            error_df = error_df.withColumn(col, F.lit(None).cast(StringType()))

        # assign output data object        
        self.output_data_objects["SyntheticErrors"].df = error_df

    
    def write(self):
        super().write()
        # write results

      
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

        if self.null_row_prob == 0:
            # TODO logging
            return df

        # 1) sample rows 2) sample columns 3) do a final join/union 
        # Sampling a new dataframe
        
        sampled_df = df.sample(self.null_row_prob, seed = self.seed)
        df_with_nulls = sampled_df

        # Selecting columns based on ratio param
        for column in self.mandatory_columns:
            df_with_nulls = df_with_nulls.withColumn(column, F.when(F.rand() < self.max_ratio_of_mandatory_columns, 
                                                              F.lit(None)).otherwise(F.col(column)))

        result_df = df\
            .join(df_with_nulls, on = ["user_id_copy", ColNames.event_id], how = "leftanti")\
                [[df.columns]]\
                .unionAll(df_with_nulls)
        
        return result_df

    def generate_nulls_in_mandatory_fields_columnwise(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
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

        if self.null_row_prob == 0:
            # TODO logging
            return df

        # 1) sample rows 2) sample columns 3) do a final join/union 
        # Sampling a new dataframe
        
        sampled_df = df.sample(self.null_row_prob, seed = self.seed)
        df_with_nulls = sampled_df

        # Selecting columns based on ratio param
        for column in self.mandatory_columns:
            
            column_null_row_prob = 0
            df_with_nulls = df[["user_id_copy", ColNames.event_id]]\
                .sample(column_null_row_prob, seed = self.seed)\
                .withColumn(column, F.lit(None))

            result_df = df\
                .join(df_with_nulls, on = ["user_id_copy", ColNames.event_id], how = "leftanti")\
                    [[df.columns]]\
                    .unionAll(df_with_nulls)
            
        return result_df
    
    
    def generate_out_of_bounds_dates(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """

        Args:
            df (pyspark.sql.DataFrame): Data of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Data where some timestamp column values are out of bounds as per config.
        """
        
        if self.out_of_bounds_prob == 0:
            # TODO logging
            return df

        # seed_param = 999 if self.config["use_fixed_seed"] else None
        events_span_in_months = pd.Timestamp(self.date_bounds[1]).month - pd.Timestamp(self.date_bounds[0]).month

        # Current idea is to 1) sample rows 2) sample columns 3) do a final join/union 4) sort
        df_not_null_dates = df.where(F.col("timestamp").isNotNull()) 

        # TODO this should account for wrong type as well
        # Current approach means that this should be run after nulls, but before wrong type generation

        df_with_sample_column = df_not_null_dates.join(
            df_not_null_dates[[ColNames.event_id, ColNames.user_id]].sample(self.out_of_bounds_prob, seed = self.seed).withColumn("out_of_bounds", F.lit(True)),
            on = [ColNames.user_id, ColNames.event_id],
            how = "left"
        ).withColumn("out_of_bounds", F.when(
            F.col("out_of_bounds").isNull(), F.lit(False)).otherwise(F.col("out_of_bounds")))

        df_with_sample_column = df_with_sample_column.withColumn("months_to_add", 
                                                                 (F.lit(1) + F.randn(self.seed))*F.lit(events_span_in_months))

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
        .unionAll(df_with_sample_column[df.columns + ["out_of_bounds"]])

        return result_df

    def cast_and_mutate_row(self, df: pyspark.sql.DataFrame, column_name: str, to_type: str, to_value: F) -> pyspark.sql.DataFrame:
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
                F.col("mutate_to_error"),
                    F.col(column_name).cast(to_type))
                        .otherwise(F.col(column_name))
            )\
            .withColumn(column_name, F.when(
                F.col("mutate_to_error"),
                    to_value)
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
        
        if self.error_prob == 0:
            # TODO logging
            return df

        # Current idea is to 1) select not null rows 2) create a sampling column 3) perform type mutation
        # This creates wrong type values on same selected rows for all mandatory columns
        # Could be improved to select non-nulls for only that specific column, and do sampling for each column seperately
        # Or to sample the probabilities for wrong types seperately?

        # Errors shouldn't be generated, where there are out of bounds records, otherwise the probabilities don't realise in the final data
        # as expected (for instance, the ratio of error records may be much smaller than the probability assigned,
        # because some nulls are re-done as errors)

        # In case the pipeline is such that out of bounds comes later, or out_of_bounds probability = 0
        if "out_of_bounds" not in df.columns:
            df = df.withColumn("out_of_bounds", F.lit(False))

        # Recast binary column
        # Cast BinaryType column to StringType with error handling

        df = df.withColumn(
            ColNames.user_id,
            F.base64(F.col(ColNames.user_id)).cast(StringType())
        ) # recasting here to enable union later

        df_not_null = df.dropna().filter(F.col("out_of_bounds") == F.lit(False))

        df = df.drop(F.col("out_of_bounds"))

        df_with_sample_column = df_not_null.join(
            df_not_null[[ColNames.event_id, ColNames.user_id]].sample(self.error_prob, seed = self.seed).withColumn("mutate_to_error", F.lit(True)),
            on = [ColNames.user_id, ColNames.event_id],
            how = "left"
        )

        # Iterate over mandatory columns to mutate the type for a sampled row
        # First cast sampled rows, then fill with values
        # TODO refactor more compactly

        for struct_schema in BronzeEventDataObject.SCHEMA:
            if struct_schema.name not in self.mandatory_columns:
                continue
            column = struct_schema.name
            col_dtype = struct_schema.dataType
            # col_dtype = [dtype for name, dtype in df_with_sample_column.dtypes if name == column][0]
            
            if col_dtype in [BinaryType()]:
                # same logic of when and cast + mutate doesn't work
                # continue
                # TODO 

                to_type = "string"
                random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)) + "_"
                to_value =  F.concat(F.lit(random_string), (F.rand() * 100).cast("int"))


            if col_dtype in [FloatType(), IntegerType()]:
                to_type = "string"
                random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)) + "_"
                to_value =  F.concat(F.lit(random_string), (F.rand() * 100).cast("int"))
            
            if column == ColNames.timestamp:
                to_type = "string"
                timezone_to = random.randint(0, 12) # statically one timezone difference
                to_value = F.concat(
                    F.substring(F.col(column), 1, 10), 
                    F.lit("T"), F.substring(F.col(column), 12, 9), 
                    F.lit(f"+0{timezone_to}:00")
                )

            df_with_sample_column = self.cast_and_mutate_row(
                df_with_sample_column,
                column, 
                to_type,
                to_value
            )
        
        # TODO check a more optional join
        result_df = df.join(df_with_sample_column, on = [ColNames.user_id, ColNames.event_id], how = "leftanti")\
            [df.columns]\
            .unionAll(df_with_sample_column[df.columns])
        
        return result_df


    def generate_optional_column(self, df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """
        Generates erronous values in optional column.

        Args:
            df (pyspark.sql.DataFrame): dataframe with null, out of bound and erronous records

        Returns:
            pyspark.sql.DataFrame: dataframe with optional column.
        """ 

        pass

        

if __name__ == "__main__":
    # Example run
    root_path = "opt/dev"
    general_config = f"{root_path}/pipe_configs/configurations/general_config.ini"
    component_config = f"{root_path}/pipe_configs/configurations/synthetic_events/synthetic_events_and_errors.ini"
    test_generator = SyntheticErrors(general_config, component_config)
    test_generator.execute()