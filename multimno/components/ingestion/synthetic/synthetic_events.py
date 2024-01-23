import random
import datetime
import string
from enum import Enum
import pandas as pd
from pyspark.sql import Row, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, explode, sha2, col, date_format, to_timestamp, lit
from pyspark.sql.types import (
    IntegerType,
    TimestampType,
    ArrayType,
    StructType,
    StructField,
    BinaryType,
    StringType,
    FloatType,
    LongType,
)

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject


# Return type for the agent records generation UDF.
agent_records_return_type = ArrayType(
    StructType(
        [
            StructField(name=ColNames.event_id, dataType=IntegerType(), nullable=False),
            StructField(name=ColNames.timestamp, dataType=TimestampType(), nullable=False),
            StructField(name=ColNames.cell_id, dataType=LongType(), nullable=True),
            StructField(name=ColNames.latitude, dataType=FloatType(), nullable=True),
            StructField(name=ColNames.longitude, dataType=FloatType(), nullable=True),
        ]
    )
)


@udf(returnType=agent_records_return_type)
def generate_agent_records(
    user_id, n_events, starting_event_id, random_seed, timestamp_generator_params, location_generator_params
):
    """
    UDF to generate records from agent parameters.
    Generates an array of (event_id, timestamp, cell_id) tuples.

    Args:
        user_id (str): _description_
        n_events (int): number of events to generate.
        starting_event_id (int): starting value for event_id column (used only internally, and not stored).
        random_seed (int): random seed that is the same for all processes throughout the module.
        timestamp_generator_params (list): list of parameters for timestamp generation, specified in config.
        location_generator_params (list): list of parameters for loaction generation, specifiec in config.

    Returns:
        zip: list of tuples with all event id-s, timestamps, cell_ids, latitude and longitude values
    """
    # Generate event id values.
    event_ids = [i for i in range(starting_event_id, starting_event_id + n_events)]

    # Generate timestamp values.
    # TODO timestamp generator types
    timestamp_generator_type = timestamp_generator_params[0]
    if timestamp_generator_type == TimestampGeneratorType.EQUAL_GAPS.value:
        starting_timestamp = timestamp_generator_params[1]
        ending_timestamp = timestamp_generator_params[2]
        gap_length_s = (ending_timestamp - starting_timestamp) / n_events
        current_timestamp = starting_timestamp
        timestamps = []
        for i in range(n_events):
            timestamps.append(current_timestamp)
            current_timestamp += gap_length_s

    # Generate location values.
    # Location is identified either by cell id or by latitude and longitude.
    location_generator_type = location_generator_params[0]
    random.seed(random_seed + user_id)
    if location_generator_type == LocationGeneratorType.RANDOM_CELL_ID.value:
        cell_id_min = location_generator_params[1]
        cell_id_max = location_generator_params[2]
        cell_ids = [random.randint(cell_id_min, cell_id_max) for i in range(n_events)]
        lats = [None for i in range(n_events)]
        lons = [None for i in range(n_events)]
    elif location_generator_type == LocationGeneratorType.RANDOM_LAT_LON.value:
        lat_min = location_generator_params[1]
        lat_max = location_generator_params[2]
        lon_min = location_generator_params[3]
        lon_max = location_generator_params[4]
        cell_ids = [None for i in range(n_events)]
        lats = [random.random() * (lat_max - lat_min) + lat_min for i in range(n_events)]
        lons = [random.random() * (lon_max - lon_min) + lon_min for i in range(n_events)]

    events = zip(event_ids, timestamps, cell_ids, lats, lons)
    return events


# Enum of location generators supported by the synthetic events generator.
class LocationGeneratorType(Enum):
    """
    Location Generator enumeration class.
    """

    RANDOM_CELL_ID = "random_cell_id"
    RANDOM_LAT_LON = "random_lat_lon"


# Enum of timestamp generators supported by the synthetic events generator.
class TimestampGeneratorType(Enum):
    """
    Timestamp Generator enumeration class.
    """

    EQUAL_GAPS = "equal_gaps"


class SyntheticEvents(Component):
    """
    Class that generates the event synthetic data. It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticEvents"

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)
        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        self.n_agents = self.config.getint(self.COMPONENT_ID, "n_agents")
        self.n_events_per_agent = self.config.getint(self.COMPONENT_ID, "n_events_per_agent")
        self.n_partitions = self.config.getint(self.COMPONENT_ID, "n_partitions")
        self.timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")
        self.starting_timestamp = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "starting_timestamp"), self.timestamp_format
        )
        self.ending_timestamp = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "ending_timestamp"), self.timestamp_format
        )

        # Handle timestamp generation parameters.
        # TODO support for other timestamp generation methods
        timestamp_generator_type_str = self.config.get(self.COMPONENT_ID, "timestamp_generator_type")
        try:
            timestamp_generator_type = TimestampGeneratorType(timestamp_generator_type_str)
        except:
            raise ValueError(
                f"Unsupported timestamp_generator_type: {timestamp_generator_type}. Supported types are: {[e.value for e in TimestampGeneratorType]}"
            )
        if timestamp_generator_type == TimestampGeneratorType.EQUAL_GAPS:
            self.timestamp_generator_params = (
                timestamp_generator_type.value,
                self.starting_timestamp,
                self.ending_timestamp,
            )

        # Handle location generation parameters.
        location_generator_type_str = self.config.get(self.COMPONENT_ID, "location_generator_type")
        try:
            locationGenerator = LocationGeneratorType(location_generator_type_str)
        except:
            raise ValueError(
                f"Unsupported location_generator_type: {location_generator_type_str}. Supported types are: {[e.value for e in LocationGeneratorType]}"
            )
        if locationGenerator == LocationGeneratorType.RANDOM_CELL_ID:
            cell_id_min = self.config.getint(self.COMPONENT_ID, "cell_id_min")
            cell_id_max = self.config.getint(self.COMPONENT_ID, "cell_id_max")
            self.location_generator_params = (locationGenerator.value, cell_id_min, cell_id_max)
        elif locationGenerator == LocationGeneratorType.RANDOM_LAT_LON:
            latitude_min = float(self.config.get(self.COMPONENT_ID, "latitude_min"))
            latitude_max = float(self.config.get(self.COMPONENT_ID, "latitude_max"))
            longitude_min = float(self.config.get(self.COMPONENT_ID, "longitude_min"))
            longitude_max = float(self.config.get(self.COMPONENT_ID, "longitude_max"))
            self.location_generator_params = (
                locationGenerator.value,
                latitude_min,
                latitude_max,
                longitude_min,
                longitude_max,
            )

        # Will we need better mcc generation later?
        self.mcc = self.config.getint(self.COMPONENT_ID, "mcc")

        # Error generation parameters.
        self.do_error_generation = self.config.getboolean(self.COMPONENT_ID, "do_error_generation")
        self.max_ratio_of_mandatory_columns = self.config.getfloat(
            self.COMPONENT_ID, "max_ratio_of_mandatory_columns_to_generate_as_null"
        )
        self.null_row_prob = self.config.getfloat(self.COMPONENT_ID, "null_row_probability")
        self.error_prob = self.config.getfloat(self.COMPONENT_ID, "data_type_error_probability")
        self.out_of_bounds_prob = self.config.getfloat(self.COMPONENT_ID, "out_of_bounds_probability")
        self.mandatory_columns = [i.name for i in BronzeEventDataObject.SCHEMA]
        self.sort_output = self.config.getboolean(self.COMPONENT_ID, "sort_output")

    def initalize_data_objects(self):
        output_records_path = self.config.get(self.COMPONENT_ID, "output_records_path")

        # TODO csv interface support needed ?
        bronze_event = BronzeEventDataObject(
            self.spark, output_records_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )  # ParquetInterface()

        self.output_data_objects = {"SyntheticEvents": bronze_event}

    def read(self):
        pass  # No input datasets are used in this component

    def transform(self):
        spark = self.spark

        # Initialize each agent, generate Spark dataframe
        agents = self.generate_agents()
        agents_df = spark.createDataFrame(agents)
        # Generate events for each agent. Since the UDF generates a list, it has to be exploded to separate the rows.
        records_df = agents_df.withColumn(
            "record_tuple",
            explode(
                generate_agent_records(
                    "user_id",
                    "n_events",
                    "starting_event_id",
                    "random_seed",
                    "timestamp_generator_params",
                    "location_generator_params",
                )
            ),
        ).select(["*", "record_tuple.*"])

        # TODO add loc_error non-null value generation.
        records_df = records_df.withColumn(ColNames.loc_error, lit(None).cast(FloatType()))

        records_df = self.calc_hashed_user_id(records_df)
        records_df = records_df.withColumn(ColNames.timestamp, col(ColNames.timestamp).cast(StringType()))
        records_df = records_df.withColumn(ColNames.cell_id, col(ColNames.cell_id).cast(StringType()))
        records_df = records_df.withColumn(ColNames.mcc, col(ColNames.mcc).cast(IntegerType()))

        # TODO use DataObject schema for selecting the columns?
        bronze_columns = [i.name for i in BronzeEventDataObject.SCHEMA]

        # TODO Should certain location columns (depending on generator params) not be created?
        # for unsupported_column in ["longitude", "latitude", "loc_error"]:
        #     bronze_columns.remove(unsupported_column)

        records_df = records_df.select(bronze_columns)

        # Transform timestamp to expected format
        records_df = records_df.withColumn(
            "timestamp", date_format(to_timestamp(col("timestamp")), format="yyyy-MM-dd'T'HH:mm:ss")
        )
        records_df = records_df.withColumn(ColNames.year, F.year(col(ColNames.timestamp)))
        records_df = records_df.withColumn(ColNames.month, F.month(col(ColNames.timestamp)))
        records_df = records_df.withColumn(ColNames.day, F.dayofmonth(col(ColNames.timestamp)))

        # If error generation is enabled, replace clean dataset with errorful dataset.
        if self.do_error_generation:
            records_df = self.generate_errors(records_df)

        # Assign output data object dataframe
        self.output_data_objects["SyntheticEvents"].df = records_df

    def write(self):
        super().write()
        # TODO Rename output directories to YYYY/MM/DD?

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
        # TODO event ids should be numbered per partition, not global?
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
                    random_seed=self.seed,
                    timestamp_generator_params=self.timestamp_generator_params,
                    location_generator_params=self.location_generator_params,
                )
            )
            starting_event_id += self.n_events_per_agent
        return agents

    def calc_hashed_user_id(self, df) -> DataFrame:
        """
        Calculates SHA2 hash of user id, takes the first 31 bits and converts them to a non-negative 32-bit integer.

        Args:
            df (pyspark.sql.DataFrame): Data of clean synthetic events with a user id column.

        Returns:
            pyspark.sql.DataFrame: Dataframe, where user_id column is transformered to a hashed value.

        """
        df = df.withColumn("ms_id_binary", col(ColNames.user_id).cast(BinaryType()))

        df = df.withColumn(ColNames.user_id, sha2(col("ms_id_binary"), numBits=256))

        df = df.drop("ms_id_binary")

        return df

    def generate_errors(self, synth_df_raw: DataFrame) -> DataFrame:
        """
        Transforms a dataframe with clean synthetic records, by calculating year, month and day columns,
        creating an event_id column, and generating all types of erronous records.
        Calls all error generation functions.

        Args:
            df (pyspark.sql.DataFrame): Data of raw and clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Dataframe, with erroneous records, according to probabilities defined in the configuration.
        """
        # Create a copy of original MSID column for final sorting and joining
        synth_df_raw = synth_df_raw.withColumn("user_id_copy", F.col(ColNames.user_id))

        synth_df_raw = synth_df_raw.withColumn(ColNames.year, F.year(F.col(ColNames.timestamp)))
        synth_df_raw = synth_df_raw.withColumn(ColNames.month, F.month(F.col(ColNames.timestamp)))
        synth_df_raw = synth_df_raw.withColumn(ColNames.day, F.dayofmonth(F.col(ColNames.timestamp)))

        # Create event id column if missing
        if ColNames.event_id not in synth_df_raw.columns:
            windowSpec = Window.partitionBy(F.col(ColNames.user_id)).orderBy(F.col(ColNames.timestamp))
            synth_df_raw = synth_df_raw.withColumn(ColNames.event_id, F.row_number().over(windowSpec))

        # TODO optional column support
        synth_df_raw = synth_df_raw.cache()
        synth_df = synth_df_raw[self.mandatory_columns + ["user_id_copy", ColNames.event_id]]

        synth_df_w_nulls = self.generate_nulls_in_mandatory_fields(synth_df)
        synth_df_w_out_of_bounds_and_nulls = self.generate_out_of_bounds_dates(synth_df_w_nulls)
        synth_df_w_out_of_bounds_nulls_errors = self.generate_erroneous_type_values(synth_df_w_out_of_bounds_and_nulls)

        synth_df_w_out_of_bounds_nulls_errors = synth_df_w_out_of_bounds_nulls_errors.join(
            synth_df_raw[["user_id_copy", ColNames.year, ColNames.month, ColNames.day, ColNames.event_id]],
            on=["user_id_copy", ColNames.event_id],
            how="left",
        )

        # Sort
        if self.sort_output:
            synth_df_w_out_of_bounds_nulls_errors = synth_df_w_out_of_bounds_nulls_errors.orderBy(
                ["user_id_copy", ColNames.event_id]
            )

        error_df = synth_df_w_out_of_bounds_nulls_errors.drop(F.col("user_id_copy")).drop(F.col(ColNames.event_id))

        return error_df

    def generate_nulls_in_mandatory_fields(self, df: DataFrame) -> DataFrame:
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
        df = df.cache()
        sampled_df = df.sample(self.null_row_prob, seed=self.seed)
        df_with_nulls = sampled_df

        # Selecting columns based on ratio param
        for column in self.mandatory_columns:
            df_with_nulls = df_with_nulls.withColumn(
                column, F.when(F.rand() < self.max_ratio_of_mandatory_columns, F.lit(None)).otherwise(F.col(column))
            )

        result_df = df.join(df_with_nulls, on=["user_id_copy", ColNames.event_id], how="leftanti")[
            [df.columns]
        ].unionAll(df_with_nulls)

        return result_df

    def generate_out_of_bounds_dates(self, df: DataFrame) -> DataFrame:
        """
        Transformers the timestamp column values to be out of bound of the selected period,
        based on probabilities from configuration.

        Args:
            df (pyspark.sql.DataFrame): Dataframe of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Data where some timestamp column values are out of bounds as per config.
        """

        if self.out_of_bounds_prob == 0:
            # TODO logging
            return df

        # approximate span in months is enough for error generation
        events_span_in_months = (pd.Timestamp(self.ending_timestamp) - pd.Timestamp(self.starting_timestamp)).days / 30

        # Current idea is to 1) sample rows 2) sample columns 3) do a final join/union 4) sort
        df = df.cache()
        df_not_null_dates = df.where(F.col("timestamp").isNotNull())

        # Current approach means that this should be run after nulls, but before wrong type generation

        df_not_null_dates = df_not_null_dates.cache()
        df_with_sample_column = df_not_null_dates.join(
            df_not_null_dates[[ColNames.event_id, ColNames.user_id]]
            .sample(self.out_of_bounds_prob, seed=self.seed)
            .withColumn("out_of_bounds", F.lit(True)),
            on=[ColNames.user_id, ColNames.event_id],
            how="left",
        ).withColumn(
            "out_of_bounds", F.when(F.col("out_of_bounds").isNull(), F.lit(False)).otherwise(F.col("out_of_bounds"))
        )

        df_with_sample_column = df_with_sample_column.withColumn(
            "months_to_add", (F.lit(1) + F.randn(self.seed)) * F.lit(events_span_in_months)
        )

        df_with_sample_column = df_with_sample_column.withColumn(
            "timestamp",
            F.when(F.col("out_of_bounds"), F.add_months(F.col("timestamp"), F.col("months_to_add"))).otherwise(
                F.col("timestamp")
            ),
        ).drop(F.col("months_to_add"))

        columns_to_error_generation = ["out_of_bounds"]

        if self.error_prob == 0:
            columns_to_error_generation = []

        result_df = (
            df.where(F.col("timestamp").isNull())
            .withColumn("out_of_bounds", F.lit(None))[df.columns + columns_to_error_generation]
            .unionAll(df_with_sample_column[df.columns + columns_to_error_generation])
        )

        return result_df

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
            ColNames.user_id, F.base64(F.col(ColNames.user_id)).cast(StringType())
        )  # recasting here to enable union later

        df = df.cache()
        df_not_null = df.dropna().filter(F.col("out_of_bounds") == F.lit(False))

        df = df.drop(F.col("out_of_bounds"))

        df_not_null = df_not_null.cache()
        df_with_sample_column = df_not_null.join(
            df_not_null[[ColNames.event_id, ColNames.user_id]]
            .sample(self.error_prob, seed=self.seed)
            .withColumn("mutate_to_error", F.lit(True)),
            on=[ColNames.user_id, ColNames.event_id],
            how="left",
        )

        # Iterate over mandatory columns to mutate the type for a sampled row
        # First cast sampled rows, then fill with values
        # TODO refactor more compactly

        for struct_schema in BronzeEventDataObject.SCHEMA:
            if struct_schema.name not in self.mandatory_columns:
                continue

            column = struct_schema.name
            col_dtype = struct_schema.dataType

            if col_dtype in [BinaryType()]:
                to_value = F.md5(F.col(column))  # numBits=224

            if col_dtype in [FloatType(), IntegerType()]:
                # changes mcc, lat, lon
                to_value = F.col(column) + ((F.rand() + F.lit(180)) * 10000).cast("int")

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

            df_with_sample_column = self.mutate_row(df_with_sample_column, column, to_value)

        # TODO check for a more optimal join
        result_df = df.join(df_with_sample_column, on=["user_id_copy", ColNames.event_id], how="leftanti")[
            df.columns
        ].unionAll(df_with_sample_column[df.columns])

        return result_df

    def mutate_row(self, df: DataFrame, column_name: str, to_value: F) -> DataFrame:
        """
        Mutates a row when mutate_to_error = True. changes the value accordingly.

        Args:
            df (pyspark.sql.DataFrame): dataframe, the rows of which to make erronous
            column_name (str): column to change
            to_value (any): pyspark sql function statement, for instance F.col(), or F.lit()

        Returns:
            pyspark.sql.DataFrame: dataframe with erronoeous rows
        """

        df = df.withColumn(column_name, F.when(F.col("mutate_to_error"), to_value).otherwise(F.col(column_name)))

        return df
