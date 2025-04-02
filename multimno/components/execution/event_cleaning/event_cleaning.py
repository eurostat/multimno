"""
Module that cleans RAW MNO Event data.
"""

from typing import List, Dict
import datetime
from functools import reduce
from multimno.core.constants.domain_names import Domains
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F
from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)
from multimno.core.data_objects.silver.silver_event_data_object import (
    SilverEventDataObject,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from multimno.core.spark_session import (
    check_if_data_path_exists,
    delete_file_or_folder,
)
from multimno.core.settings import (
    CONFIG_BRONZE_PATHS_KEY,
    CONFIG_SILVER_PATHS_KEY,
    GENERAL_CONFIG_KEY,
)
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import ErrorTypes
from multimno.core.constants.transformations import Transformations
import multimno.core.utils as utils
from multimno.core.log import get_execution_stats


class EventCleaning(Component):
    """
    Class that cleans MNO Event data
    """

    COMPONENT_ID = "EventCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.do_same_location_deduplication = self.config.getboolean(
            self.COMPONENT_ID,
            "do_same_location_deduplication",
            fallback=False,
        )

        self.do_bounding_box_filtering = self.config.getboolean(
            self.COMPONENT_ID,
            "do_bounding_box_filtering",
            fallback=False,
        )

        self.do_cell_id_length_filtering = self.config.getboolean(
            self.COMPONENT_ID,
            "do_cell_id_length_filtering",
            fallback=False,
        )

        self.do_timestamp_conversion_to_utc = self.config.getboolean(
            self.COMPONENT_ID,
            "do_timestamp_conversion_to_utc",
            fallback=False,
        )

        self.timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")
        self.local_timezone = self.config.get(GENERAL_CONFIG_KEY, "local_timezone")
        self.local_mcc = self.config.getint(GENERAL_CONFIG_KEY, "local_mcc")
        self.bbox = self.config.geteval(self.COMPONENT_ID, "bounding_box")

    def initalize_data_objects(self):
        # Input
        self.bronze_event_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")

        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        self.number_of_partitions = self.config.get(self.COMPONENT_ID, "number_of_partitions")

        self.data_period_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        # List of datetime.dates for which to perform cleaning of the raw network input data.
        # Notice that date_period_end is included
        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        # Create all input data objects
        self.input_event_data_objects = []
        self.dates_to_process = []
        for date in self.data_period_dates:
            path = f"{self.bronze_event_path}/year={date.year}/month={date.month}/day={date.day}"
            if check_if_data_path_exists(self.spark, path):
                self.dates_to_process.append(date)
                self.input_event_data_objects.append(BronzeEventDataObject(self.spark, path))
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")

        # Output
        self.output_data_objects = {}

        outputs = {
            "event_syntactic_quality_metrics_by_column": SilverEventDataSyntacticQualityMetricsByColumn,
            "event_syntactic_quality_metrics_frequency_distribution": SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
            "event_data_silver": SilverEventDataObject,
        }

        for key, value in outputs.items():
            path = self.config.get(CONFIG_SILVER_PATHS_KEY, key)
            if self.clear_destination_directory:
                delete_file_or_folder(self.spark, path)
            self.output_data_objects[value.ID] = value(self.spark, path)

    def read(self):
        self.current_input_do.read()

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        for input_do, current_date in zip(self.input_event_data_objects, self.dates_to_process):
            self.current_date = current_date
            self.logger.info(f"Reading from path {input_do.default_path}")
            self.current_input_do = input_do
            self.read()
            self.transform()  # Transforms the input_df
            self.write()
            # after each chunk processing clear all Cache to free memory and disk
            self.spark.catalog.clearCache()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        df_events = self.current_input_do.df

        # check nulls
        check_null_columns = [ColNames.user_id, ColNames.timestamp, ColNames.mcc, ColNames.mnc]

        df_events = self.flag_nulls(df_events, check_null_columns)

        # add user_id_modulo
        # The concept of device demultiplex is implemented here
        # 1) Creates a modulo column, 2) repartitions according to it 3) sorts data within partitions

        df_events = self.calculate_user_id_modulo(df_events, self.number_of_partitions)
        df_events = df_events.repartition(ColNames.user_id_modulo)
        df_events = df_events.sortWithinPartitions(ColNames.user_id, ColNames.timestamp)

        # parse timestamp
        df_events = self.parse_timestamp(df_events, self.timestamp_format)

        # flag out of range limits
        cols_range_limits = {
            ColNames.mcc: [100, 999],
            ColNames.plmn: [10000, 99999],
        }

        df_events = self.flag_out_of_range_limits(df_events, cols_range_limits)

        if self.do_bounding_box_filtering:
            cols_range_limits = {
                ColNames.latitude: [self.bbox[1], self.bbox[3]],
                ColNames.longitude: [self.bbox[0], self.bbox[2]],
            }
            df_events = self.flag_out_of_range_limits(df_events, cols_range_limits)

        # flag out of length limits
        cols_length_limits = {ColNames.mnc: [2, 3]}

        df_events = self.flag_out_of_length_limits(df_events, cols_length_limits)

        if self.do_cell_id_length_filtering:
            cols_length_limits = {ColNames.cell_id: [14, 15]}
            df_events = self.flag_out_of_length_limits(df_events, cols_length_limits)

        # flag timestamp out of bounds
        df_events = self.flag_timestamp_out_of_bounds(
            df_events, self.current_date, self.current_date + datetime.timedelta(days=1)
        )

        # flag missing network info
        df_events = self.flag_missing_mno_info(df_events)

        # assign domain
        df_events = self.assign_domain(df_events, self.local_mcc)

        # flag missing location
        df_events = self.flag_missing_location(df_events)

        # flag same location duplicates
        if self.do_same_location_deduplication:
            df_events = self.flag_same_location_duplicates(df_events)

        # add no error flag columns
        df_events = self.add_no_error_flag_columns(df_events)

        df_events.persist(StorageLevel.MEMORY_AND_DISK)

        # get metrics by flag column
        output_qa_by_column = self.get_metrics_by_flag_column(df_events)

        output_qa_by_column = output_qa_by_column.withColumns(
            {
                ColNames.result_timestamp: F.lit(F.current_timestamp()),
                ColNames.date: F.to_date(F.lit(self.current_date)),
            }
        )

        output_qa_by_column = utils.apply_schema_casting(
            output_qa_by_column, SilverEventDataSyntacticQualityMetricsByColumn.SCHEMA
        )
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID].df = output_qa_by_column

        # get frequency metrics
        frequency_metrics = self.get_frequency_metrics(df_events)

        frequency_metrics = frequency_metrics.withColumns(
            {
                ColNames.result_timestamp: F.lit(F.current_timestamp()),
                ColNames.date: F.to_date(F.lit(self.current_date)),
            }
        )

        frequency_metrics = utils.apply_schema_casting(
            frequency_metrics, SilverEventDataSyntacticQualityMetricsFrequencyDistribution.SCHEMA
        )
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID].df = frequency_metrics

        df_events = df_events.filter(F.col("to_preserve"))

        if self.do_timestamp_conversion_to_utc:
            df_events = self.convert_to_utc(df_events, self.local_timezone)

        df_events = df_events.withColumns(
            {
                ColNames.year: F.year(ColNames.timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.timestamp).cast("tinyint"),
            }
        )

        df_events = utils.apply_schema_casting(df_events, SilverEventDataObject.SCHEMA)
        df_events = df_events.repartition(*SilverEventDataObject.PARTITION_COLUMNS)

        self.output_data_objects[SilverEventDataObject.ID].df = df_events

    @staticmethod
    def flag_nulls(
        sdf: DataFrame,
        filter_columns: List[str],
    ) -> DataFrame:
        """
        Marks rows which include nulls in the filter columns

        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible nulls values
            filter_columns (List[str], optional): columns to check for nulls
        Returns:
            pyspark.sql.dataframe.DataFrame: df where rows with null values in filter columns are flagged
        """

        sdf = sdf.withColumns({f"{col}_flag_{ErrorTypes.NULL_VALUE}": F.col(col).isNull() for col in filter_columns})

        return sdf

    @staticmethod
    def parse_timestamp(
        sdf: DataFrame,
        timestamp_format: str,
    ) -> DataFrame:
        """
        Based on config params timestampt format and input timezone
        convert timestamp column from string to timestamp type.
        Flag succesful timestamp transformations and errors.

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with timestamp column
            timestamp_format (str): expected string format to use in time conversion

        Returns:
            pyspark.sql.dataframe.DataFrame: df with time column parsed to timestamp
        """

        sdf = sdf.withColumn(
            f"{ColNames.timestamp}_parsed",
            F.to_timestamp(ColNames.timestamp, timestamp_format),
        )

        sdf = sdf.withColumn(
            f"{ColNames.timestamp}_flag_{ErrorTypes.CANNOT_PARSE}",
            F.when(
                F.col(ColNames.timestamp).isNotNull() & F.col(f"{ColNames.timestamp}_parsed").isNull(),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        sdf = sdf.withColumn(
            ColNames.timestamp,
            F.col(f"{ColNames.timestamp}_parsed"),
        ).drop(f"{ColNames.timestamp}_parsed")

        return sdf

    @staticmethod
    def flag_out_of_range_limits(
        sdf: DataFrame,
        cols_range_limits: Dict[str, List[int]],
    ) -> DataFrame:
        """
        Checks if values in columns are within the specified range

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with columns to check
            cols_range_limits (Dict[str, List[int]]): dictionary with column names as keys and lists of min and max values as values

        Returns:
            pyspark.sql.dataframe.DataFrame: df with columns checked for range limits
        """

        for col_name, limits in cols_range_limits.items():
            sdf = sdf.withColumn(
                f"{col_name}_flag_{ErrorTypes.OUT_OF_RANGE}",
                F.when(
                    F.col(col_name).isNotNull() & ~F.col(col_name).between(limits[0], limits[1]),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
        return sdf

    @staticmethod
    def flag_out_of_length_limits(
        sdf: DataFrame,
        cols_length_limits: Dict[str, List[int]],
    ) -> DataFrame:
        """
        Checks if values in columns are within the specified characters length

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with columns to check
            cols_length_limits (Dict[str, List[int]]): dictionary with column names as keys and lists of min and max values as values

        Returns:
            pyspark.sql.dataframe.DataFrame: df with columns checked for range limits
        """

        for col_name, limits in cols_length_limits.items():
            sdf = sdf.withColumn(
                f"{col_name}_flag_{ErrorTypes.OUT_OF_RANGE}",
                F.when(
                    F.col(col_name).isNotNull() & ~F.length(F.col(col_name)).between(limits[0], limits[1]),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )

        return sdf

    @staticmethod
    def flag_timestamp_out_of_bounds(
        sdf: DataFrame,
        start_date: str,
        end_date: str,
    ) -> DataFrame:
        """
        Flags rows with timestamps outside of the specified date bounds

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with timestamp column
            start_date (str): start date of the data
            end_date (str): end date of the data

        Returns:
            pyspark.sql.dataframe.DataFrame: df with rows flagged if timestamp is out of bounds
        """

        sdf = sdf.withColumn(
            f"{ColNames.timestamp}_flag_{ErrorTypes.OUT_OF_RANGE}",
            F.when(
                F.col(f"{ColNames.timestamp}").isNotNull()
                & ~F.col(f"{ColNames.timestamp}").between(start_date, end_date),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return sdf

    @staticmethod
    def flag_missing_mno_info(
        sdf: DataFrame,
    ) -> DataFrame:
        """
        Flags rows with missing mno network information

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with network columns
            network_columns (List[str]): columns with network information

        Returns:
            pyspark.sql.dataframe.DataFrame: df with rows flagged if network info is missing
        """

        sdf = sdf.withColumn(
            f"no_mno_info_flag_{ErrorTypes.NO_MNO_INFO}",
            F.when(
                F.col(ColNames.mcc).isNull() & F.col(ColNames.mnc).isNull() & F.col(ColNames.plmn).isNull(),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return sdf

    @staticmethod
    def assign_domain(
        sdf: DataFrame,
        local_mcc: int,
    ) -> DataFrame:
        """
        Assigns domain to rows

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with domain column
            local_mcc (int): local_mcc value

        Returns:
            pyspark.sql.dataframe.DataFrame: df with domain column assigned
        """
        sdf = sdf.withColumn(
            ColNames.domain,
            F.when(
                (F.col(ColNames.plmn).isNotNull()) & (F.col(ColNames.plmn).substr(1, 3) != F.lit(local_mcc)),
                Domains.OUTBOUND,
            ).otherwise(F.when(F.col(ColNames.mcc) == local_mcc, Domains.DOMESTIC).otherwise(Domains.INBOUND)),
        )
        return sdf

    @staticmethod
    def flag_missing_location(
        sdf: DataFrame,
    ) -> DataFrame:
        """
        Flags rows with missing location information

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with location columns

        Returns:
            pyspark.sql.dataframe.DataFrame: df with rows flagged if location info is missing
        """

        sdf = sdf.withColumn(
            f"no_location_flag_{ErrorTypes.NO_LOCATION_INFO}",
            F.when(
                (
                    (
                        F.col(ColNames.latitude).isNull()
                        & F.col(ColNames.longitude).isNull()
                        & F.col(ColNames.cell_id).isNull()
                        & ((F.col(ColNames.domain) == Domains.DOMESTIC) | (F.col(ColNames.domain) == Domains.INBOUND))
                    )
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )

        return sdf

    @staticmethod
    def flag_same_location_duplicates(
        df: DataFrame,
    ) -> DataFrame:
        """
        Flag rows that have identical records for
        timestamp, cell_id, longitude, latitude, plmn and user_id.
        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible duplicates
        Returns:
            pyspark.sql.dataframe.DataFrame: df duplicate
            in terms of user_id, cell_id, latitutde, longitude, plmn
            and timestamp combination flagged
        """
        window_spec = Window.partitionBy(ColNames.user_id_modulo, ColNames.user_id).orderBy(F.col(ColNames.timestamp))
        # Use lag to get the previous row's values
        df = df.withColumns(
            {
                "prev_timestamp": F.lag(ColNames.timestamp).over(window_spec),
                "prev_cell_id": F.lag(ColNames.cell_id).over(window_spec),
                "prev_latitude": F.lag(ColNames.latitude).over(window_spec),
                "prev_longitude": F.lag(ColNames.longitude).over(window_spec),
                "prev_plmn": F.lag(ColNames.plmn).over(window_spec),
            }
        )
        # Flag the current row if the conditions are met
        df = df.withColumn(
            f"duplicated_flag_{ErrorTypes.DUPLICATED}",
            F.when(
                (F.col(ColNames.timestamp) == F.col("prev_timestamp"))
                & (
                    F.coalesce(F.col(ColNames.cell_id), F.lit("")).cast("string")
                    == F.coalesce(F.col("prev_cell_id"), F.lit("")).cast("string")
                )
                & (
                    F.coalesce(F.col(ColNames.latitude), F.lit(0.0)).cast("double")
                    == F.coalesce(F.col("prev_latitude"), F.lit(0.0)).cast("double")
                )
                & (
                    F.coalesce(F.col(ColNames.longitude), F.lit(0.0)).cast("double")
                    == F.coalesce(F.col("prev_longitude"), F.lit(0.0)).cast("double")
                )
                & (
                    F.coalesce(F.col(ColNames.plmn), F.lit("")).cast("string")
                    == F.coalesce(F.col("prev_plmn"), F.lit("")).cast("string")
                ),
                F.lit(True),
            ).otherwise(F.lit(False)),
        ).drop("prev_timestamp", "prev_cell_id", "prev_latitude", "prev_longitude", "prev_plmn")

        return df

    @staticmethod
    def add_no_error_flag_columns(sdf: DataFrame):
        """
        Add columns with 'no error' flags for each column

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with error flag columns

        Returns:
            List[str]: list of columns that are not flagged as errors
        """

        flag_columns = [col for col in sdf.columns if "flag" in col]

        base_columns = list(set(col.split("_flag")[0] for col in flag_columns))

        column_groups = dict()
        for col in base_columns:
            # for each auxiliar column
            column_groups[col] = []
            for cc in flag_columns:
                # if it is related to the DO's column:
                if col == cc.split("_flag")[0]:
                    column_groups[col].append(F.col(cc))

        column_conditions = {col: reduce(lambda a, b: a | b, column_groups[col]) for col in column_groups}

        field_without_errors = {col: ~column_conditions[col] for col in column_conditions}

        sdf = sdf.withColumns(
            {f"{col}_flag_{ErrorTypes.NO_ERROR}": field_without_errors[col] for col in field_without_errors}
        )

        sdf = sdf.withColumn(
            "to_preserve", reduce(lambda a, b: a & b, [field_without_errors[col] for col in field_without_errors])
        )

        return sdf

    @staticmethod
    def get_metrics_by_flag_column(sdf):
        """
        Get the count of flagged errors for each column

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with error columns

        Returns:
            dict: dictionary with column names as keys and error counts as values
        """
        flag_columns = [col for col in sdf.columns if "flag" in col]

        flag_counts = sdf.agg(*[F.sum(F.col(col).cast("int")).alias(col) for col in flag_columns])

        # Unpivot the true_counts DataFrame
        metrics_sdf = flag_counts.unpivot([], flag_columns, "flag_column", ColNames.value)

        metrics_sdf = (
            metrics_sdf.withColumns(
                {
                    ColNames.variable: F.split(F.col("flag_column"), "_flag_").getItem(0),
                    ColNames.type_of_error: F.split(F.col("flag_column"), "_flag_").getItem(1),
                }
            )
            .drop("flag_column")
            .orderBy(ColNames.variable)
        )

        return metrics_sdf

    @staticmethod
    def get_frequency_metrics(sdf: DataFrame) -> DataFrame:
        """
        Get total row counts per user_id and cell_id before
        filtering and after filtering (rows without errors)

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with columns to count

        Returns:
            pyspark.sql.dataframe.DataFrame: dataframe with frequency counts
        """

        frequency_metrics = sdf.groupBy(ColNames.user_id, ColNames.cell_id).agg(
            F.count("*").alias(ColNames.initial_frequency),
            F.sum(F.col("to_preserve").cast("int")).alias(ColNames.final_frequency),
        )

        return frequency_metrics

    @staticmethod
    def convert_to_utc(
        sdf: DataFrame,
        local_timezone: str,
    ) -> DataFrame:
        """
        Converts timestamp column to UTC timezone

        Args:
            sdf (pyspark.sql.dataframe.DataFrame): df with timestamp column
            local_timezone (str): timezone of the input data

        Returns:
            pyspark.sql.dataframe.DataFrame: df with timestamp column converted to UTC
        """

        sdf = sdf.withColumn(
            ColNames.timestamp,
            F.to_utc_timestamp(ColNames.timestamp, local_timezone),
        )

        return sdf

    @staticmethod
    def calculate_user_id_modulo(
        df: DataFrame,
        modulo_value: int,
        hex_truncation_end: int = 12,
    ) -> DataFrame:
        """Calculates the extra column user_id_modulo, as the result of the modulo function
        applied on the binary user id column. The modulo value will affect the number of
        partitions in the final output.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with user_id column
            modulo_value (int): modulo value to be used when dividing user id.
            hex_truncation_end (int): to which character truncate the hex, before sending it to conv function
                and then to modulo. Anything upward of 13 is likely to result in distributional issues,
                as the modulo value might not correspond to the number of final partitions.

        Returns:
            pyspark.sql.dataframe.DataFrame: dataframe with user_id_modulo column.
        """

        # TODO make hex truncation (substring parameters) as configurable by user?

        df = df.withColumn(
            ColNames.user_id_modulo,
            F.conv(
                F.substring(F.hex(F.col(ColNames.user_id)), 1, hex_truncation_end),
                16,
                10,
            ).cast("long")
            % F.lit(modulo_value).cast("bigint"),
        )

        return df
