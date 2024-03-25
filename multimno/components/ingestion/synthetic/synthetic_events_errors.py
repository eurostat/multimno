"""
Module that adds errors to synthetic MNO event data.
"""

import random
import datetime
import string
import pandas as pd
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    IntegerType,
    BinaryType,
    StringType,
    FloatType,
)

from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject


class SyntheticEventsErrors(Component):
    """
    Class that handles adding errors to synthetic data. It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticEventsErrors"

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)

        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        self.timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")

        # Error generation parameters.
        self.do_error_generation = self.config.getboolean(self.COMPONENT_ID, "do_error_generation")
        self.max_ratio_of_mandatory_columns = self.config.getfloat(
            self.COMPONENT_ID, "max_ratio_of_mandatory_columns_to_generate_as_null"
        )
        self.null_row_prob = self.config.getfloat(self.COMPONENT_ID, "null_row_probability")
        self.error_prob = self.config.getfloat(self.COMPONENT_ID, "data_type_error_probability")
        self.out_of_bounds_prob = self.config.getfloat(self.COMPONENT_ID, "out_of_bounds_probability")
        self.starting_timestamp = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "starting_timestamp"), self.timestamp_format
        )
        self.ending_timestamp = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "ending_timestamp"), self.timestamp_format
        )
        self.mandatory_columns = [i.name for i in BronzeEventDataObject.SCHEMA]
        # Do not allow error generation on year-month-day columns to avoid partitioning errors
        self.error_generation_allowed_columns = set(self.mandatory_columns) - set(
            [ColNames.year, ColNames.month, ColNames.day]
        )

        # self.sort_output = self.config.getboolean(self.COMPONENT_ID, "sort_output")

    def initalize_data_objects(self):
        # Input (synthetic_events results)
        input_events_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        input_bronze_event = BronzeEventDataObject(
            self.spark, input_events_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        self.input_data_objects = {BronzeEventDataObject.ID: input_bronze_event}

        # Output
        output_records_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_with_err_data_bronze")
        output_bronze_event = BronzeEventDataObject(
            self.spark, output_records_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )
        self.output_data_objects = {"SyntheticErrors": output_bronze_event}

    def transform(self):
        # Get input events df
        records_df = self.input_data_objects[BronzeEventDataObject.ID].df

        # Create a copy of original MSID column for final sorting and joining
        records_df = records_df.withColumn("user_id_copy", F.col(ColNames.user_id))

        bronze_columns = [i.name for i in BronzeEventDataObject.SCHEMA]

        records_df = records_df.select(bronze_columns + ["user_id_copy"])

        # If error generation is enabled, replace clean dataset with errorful dataset.
        if self.do_error_generation:
            records_df = self.generate_errors(records_df)

        records_df = records_df.drop(F.col("user_id_copy"))

        # records_df = records_df.select(bronze_columns)

        # Assign output data object dataframe
        self.output_data_objects["SyntheticErrors"].df = records_df

    def calc_hashed_user_id(self, df) -> DataFrame:
        """
        Calculates SHA2 hash of user id, takes the first 31 bits and converts them to a non-negative 32-bit integer.

        Args:
            df (pyspark.sql.DataFrame): Data of clean synthetic events with a user id column.

        Returns:
            pyspark.sql.DataFrame: Dataframe, where user_id column is transformered to a hashed value.

        """
        # TODO: is this method used?
        df = df.withColumn("hashed_user_id", F.sha2(df[ColNames.user_id].cast("string"), 256))
        df = df.withColumn(ColNames.user_id, F.unhex(F.col("hashed_user_id")))
        df = df.drop("hashed_user_id")

        return df

    def generate_errors(self, synth_df_raw: DataFrame) -> DataFrame:
        """
        Transforms a dataframe with clean synthetic records, by calculating year, month and day columns,
        creating an event_id column, and generating all types of erronous records.
        Calls all error generation functions.

        Args:
            synth_df_raw (pyspark.sql.DataFrame): Data of raw and clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Dataframe, with erroneous records, according to probabilities defined in the configuration.
        """

        synth_df_raw = synth_df_raw.withColumn(ColNames.year, F.year(F.col(ColNames.timestamp)))
        synth_df_raw = synth_df_raw.withColumn(ColNames.month, F.month(F.col(ColNames.timestamp)))
        synth_df_raw = synth_df_raw.withColumn(ColNames.day, F.dayofmonth(F.col(ColNames.timestamp)))
        synth_df_raw = synth_df_raw.withColumn("is_modified", F.lit(False))

        synth_df = synth_df_raw.cache()

        synth_df = self.generate_nulls_in_mandatory_fields(synth_df)
        synth_df = self.generate_out_of_bounds_dates(synth_df)
        synth_df = self.generate_erroneous_type_values(synth_df)

        synth_df = synth_df.drop("is_modified")
        return synth_df

    def generate_nulls_in_mandatory_fields(self, df: DataFrame) -> DataFrame:
        """
        Generates null values in some fields of some rows based on configuration parameters.

        Args:
            df (pyspark.sql.DataFrame): clean synthetic data

        Returns:
            pyspark.sql.DataFrame: synthetic records dataframe with nulls in some columns of some rows
        """

        # Two probability parameters from config apply:
        # First one sets how many rows (as fraction of all rows) are selected for possible value nulling.
        # Second one sets the likelyhood for each column to be set to null.
        # If 1.0, then all mandatory columns of each selected row will be nulled.

        if self.null_row_prob == 0.0:
            # TODO logging
            return df

        # Split input dataframe to unchanged and changed portions
        df = df.cache()
        error_row_prob = self.null_row_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = df.randomSplit([unchanged_row_prob, error_row_prob], seed=self.seed)

        # Randomly select one column to be set as null to ensure at least one column is nulled.
        error_rows_df = error_rows_df.withColumn(
            "rand_selection", F.floor(F.rand() * (1 + len(self.error_generation_allowed_columns)))
        )
        # Randomly set some column values of rows to null, likelyhood based on ratio confing param.
        for i, column in enumerate(self.error_generation_allowed_columns):
            error_rows_df = error_rows_df.withColumn(
                column,
                F.when(
                    (F.rand(seed=self.seed) < self.max_ratio_of_mandatory_columns) | (F.col("rand_selection") == i),
                    F.lit(None),
                ).otherwise(F.col(column)),
            )
        error_rows_df = error_rows_df.drop("rand_selection")
        error_rows_df = error_rows_df.withColumn("is_modified", lit(True))

        # Re-combine unchanged and changed rows of the dataframe.
        return unchanged_rows_df.union(error_rows_df)

    def generate_out_of_bounds_dates(self, df: DataFrame) -> DataFrame:
        """
        Transforms the timestamp column values to be out of bound of the selected period,
        based on probabilities from configuration.
        Only rows with non-null timestamp values can become altered here.

        Args:
            df (pyspark.sql.DataFrame): Dataframe of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Data where some timestamp column values are out of bounds as per config.
        """

        if self.out_of_bounds_prob == 0.0:
            # TODO logging
            return df

        # Calculate approximate span in months from config parameters.
        # The parameters should approximately cover the range of "clean" dates, so that the erroneous values can be generated outside the range.
        events_span_in_months = max(
            1, (pd.Timestamp(self.ending_timestamp) - pd.Timestamp(self.starting_timestamp)).days / 30
        )

        # Split rows by null/non-null timestamp.
        df = df.cache()
        null_timestamp_df = df.where(F.col(ColNames.timestamp).isNull())
        nonnull_timestamp_df = df.where(F.col(ColNames.timestamp).isNotNull())
        df.unpersist()

        # From non-null timestamp rows, select a subset for adding errors to, depending on config parameter.
        nonnull_timestamp_df = nonnull_timestamp_df.cache()
        error_row_prob = self.out_of_bounds_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = nonnull_timestamp_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed
        )
        # Combine null timestamp rows and not-modified non-null timestamp rows.
        unchanged_rows_df = unchanged_rows_df.union(null_timestamp_df)

        # Add months offset to error rows to make their timestamp values become outside expected range.
        months_to_add_col = (F.lit(2) + F.rand(self.seed)) * F.lit(events_span_in_months)
        modified_date_col = F.add_months(F.col(ColNames.timestamp), months_to_add_col)
        time_col = F.date_format(F.col(ColNames.timestamp), " HH:mm:ss")
        error_rows_df = error_rows_df.withColumn(ColNames.timestamp, F.concat(modified_date_col, time_col))
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))

        # Combine changed and unchanged rows dataframes.
        return unchanged_rows_df.union(error_rows_df)

    def generate_erroneous_type_values(self, df: DataFrame) -> DataFrame:
        """
        Generates errors for sampled rows. Errors are custom defined, for instance a random string, or corrupt timestamp.
        Does not cast the columns to a different type.

        Args:
            df (pyspark.sql.DataFrame): dataframe that may have out of bound and null records.

        Returns:
            pyspark.sql.DataFrame: dataframe with erroneous rows, and possibly, with nulls and out of bound records.
        """

        if self.error_prob == 0:
            # TODO logging
            return df

        # Split dataframe by whether the row has been modified during the error-adding process already.
        # Already errored rows do not get further changes.
        df = df.cache()
        previously_modified_rows_df = df.where(F.col("is_modified"))
        unmodified_rows_df = df.where(~(F.col("is_modified")))
        df.unpersist()

        # From unmodified rows, select a subset for adding errors to, depending on config parameter.
        unmodified_rows_df = unmodified_rows_df.cache()
        error_row_prob = self.error_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = unmodified_rows_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed
        )
        # Gather rows that are not modified in this step (combine previously-modified rows and not-selected unmodified rows).
        unchanged_rows_df = unchanged_rows_df.union(previously_modified_rows_df)

        # Iterate over mandatory columns to mutate the value, depending on column data type.
        for struct_schema in BronzeEventDataObject.SCHEMA:
            if struct_schema.name not in self.error_generation_allowed_columns:
                continue

            column = struct_schema.name
            col_dtype = struct_schema.dataType

            if col_dtype in [BinaryType()]:
                # md5 is a smaller hash,
                to_value = F.unhex(F.md5(F.base64(F.col(column)).cast(StringType())))

            if col_dtype in [FloatType(), IntegerType()]:
                # changes mcc, lat, lon
                to_value = (F.col(column) + ((F.rand() + F.lit(180)) * 10000)).cast("int")

            if column == ColNames.timestamp and col_dtype == StringType():
                # Timezone difference manipulation may be performed here, if cleaning module were to support it.
                # statically one timezone difference
                # timezone_to = random.randint(0, 12)
                to_value = F.concat(
                    F.substring(F.col(column), 1, 10),
                    F.lit("T"),
                    F.substring(F.col(column), 12, 9),
                    # TODO: Temporary remove of timezone addition as cleaning
                    # module does not support it
                    # F.lit(f"+0{timezone_to}:00")
                )

            if column == ColNames.cell_id and col_dtype == StringType():
                random_string = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)) + "_"
                to_value = F.concat(F.lit(random_string), (F.rand() * 100).cast("int"))

            error_rows_df = error_rows_df.withColumn(column, to_value)
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))
        return unchanged_rows_df.union(error_rows_df)
