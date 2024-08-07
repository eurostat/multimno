"""
Module that cleans RAW MNO Event data.
"""

from collections import defaultdict

import pandas as pd
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ShortType,
)

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
)
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import ErrorTypes
from multimno.core.constants.transformations import Transformations


class EventCleaning(Component):
    """
    Class that cleans MNO Event data
    """

    COMPONENT_ID = "EventCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.timestamp_format = self.config.get(EventCleaning.COMPONENT_ID, "timestamp_format")
        self.input_timezone = self.config.get(EventCleaning.COMPONENT_ID, "input_timezone")
        self.local_mcc = self.config.getint(EventCleaning.COMPONENT_ID, "local_mcc")

        self.do_bounding_box_filtering = self.config.getboolean(
            EventCleaning.COMPONENT_ID,
            "do_bounding_box_filtering",
            fallback=False,
        )

        self.do_same_location_deduplication = self.config.getboolean(
            EventCleaning.COMPONENT_ID,
            "do_same_location_deduplication",
            fallback=False,
        )

        self.bounding_box = self.config.geteval(EventCleaning.COMPONENT_ID, "bounding_box")

        self.spark_data_folder_date_format = self.config.get(
            EventCleaning.COMPONENT_ID, "spark_data_folder_date_format"
        )

    def initalize_data_objects(self):
        # Input
        self.bronze_event_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")

        self.data_period_start = self.config.get(EventCleaning.COMPONENT_ID, "data_period_start")
        self.data_period_end = self.config.get(EventCleaning.COMPONENT_ID, "data_period_end")
        self.data_folder_date_format = self.config.get(EventCleaning.COMPONENT_ID, "data_folder_date_format")
        self.clear_destination_directory = self.config.get(EventCleaning.COMPONENT_ID, "clear_destination_directory")
        self.number_of_partitions = self.config.get(EventCleaning.COMPONENT_ID, "number_of_partitions")

        # Create all possible dates between start and end
        # It is suggested that data is already separated in date folders
        # with names following self.data_folder_date_format (e.g. 20230101)
        sdate = pd.to_datetime(self.data_period_start)
        edate = pd.to_datetime(self.data_period_end)
        self.to_process_dates = list(pd.date_range(sdate, edate, freq="d"))

        # Create all input data objects
        self.input_event_data_objects = []
        self.dates_to_process = []
        for date in self.to_process_dates:
            path = f"{self.bronze_event_path}/year={date.year}/month={date.month}/day={date.day}"
            if check_if_data_path_exists(self.spark, path):
                self.dates_to_process.append(date)
                self.input_event_data_objects.append(BronzeEventDataObject(self.spark, path))
            else:
                self.logger.warning(f"Expected path {path} to exist but it does not")
        # Output
        self.output_data_objects = {}

        silver_event_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver")
        silver_event_do = SilverEventDataObject(self.spark, silver_event_path)
        self.output_data_objects[SilverEventDataObject.ID] = silver_event_do
        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, silver_event_do.default_path)

        event_syntactic_quality_metrics_by_column_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_syntactic_quality_metrics_by_column"
        )
        event_syntactic_quality_metrics_by_column = SilverEventDataSyntacticQualityMetricsByColumn(
            self.spark, event_syntactic_quality_metrics_by_column_path
        )
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID] = (
            event_syntactic_quality_metrics_by_column
        )
        if self.clear_destination_directory:
            delete_file_or_folder(
                self.spark,
                event_syntactic_quality_metrics_by_column.default_path,
            )

        self.output_qa_by_column = event_syntactic_quality_metrics_by_column

        event_syntactic_quality_metrics_frequency_distribution_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY,
            "event_syntactic_quality_metrics_frequency_distribution",
        )
        event_syntactic_quality_metrics_frequency_distribution = (
            SilverEventDataSyntacticQualityMetricsFrequencyDistribution(
                self.spark,
                event_syntactic_quality_metrics_frequency_distribution_path,
            )
        )
        if self.clear_destination_directory:
            delete_file_or_folder(
                self.spark,
                event_syntactic_quality_metrics_frequency_distribution.default_path,
            )

        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID] = (
            event_syntactic_quality_metrics_frequency_distribution
        )
        # this instance of SilverEventDataSyntacticQualityMetricsFrequencyDistribution class
        # will be used to write frequency distrobution of each preprocessing date (chunk)
        # the path argument will be changed dynamically
        self.output_qa_freq_distribution = event_syntactic_quality_metrics_frequency_distribution

    def read(self):
        self.current_input_do.read()

    def write(self):
        self.output_data_objects[SilverEventDataObject.ID].write()
        self.save_syntactic_quality_metrics_frequency_distribution()
        self.save_syntactic_quality_metrics_by_column()

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        for input_do, current_date in zip(self.input_event_data_objects, self.dates_to_process):
            self.current_date = current_date
            self.logger.info(f"Reading from path {input_do.default_path}")
            self.current_input_do = input_do
            self.read()
            self.transform()  # Transforms the input_df
            self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        df_events = self.current_input_do.df
        # cache before each filter function because we appply action count()
        df_events = df_events.cache()

        self.quality_metrics_distribution_before = df_events.groupBy(ColNames.cell_id, ColNames.user_id).agg(
            F.count("*").alias(ColNames.initial_frequency)
        )

        df_events = self.filter_nulls_and_update_qa(
            df_events,
            [ColNames.user_id, ColNames.timestamp],
            self.output_qa_by_column.error_and_transformation_counts,
        )

        # Check MCC, MNC, PLMN and add domain column
        df_events = self.filter_invalid_domain_columns_and_update_qa(
            df_events, self.output_qa_by_column.error_and_transformation_counts
        )

        # already cached in previous function
        df_events = self.filter_null_locations_and_update_qa(
            df_events, self.output_qa_by_column.error_and_transformation_counts
        )

        df_events = df_events.cache()

        # Remove rows with invalid cell_ids
        df_events = self.filter_invalid_cell_id_and_update_qa(
            df_events, self.output_qa_by_column.error_and_transformation_counts
        )

        df_events = df_events.cache()

        df_events = self.convert_time_column_to_timestamp_and_update_qa(
            df_events,
            self.timestamp_format,
            self.input_timezone,
            self.output_qa_by_column.error_and_transformation_counts,
        )

        df_events = df_events.cache()
        # TODO: discuss is this step even needed (did since it was in Method description)
        df_events = self.data_period_filter_and_update_qa(
            df_events,
            self.data_period_start,
            self.data_period_end,
            self.output_qa_by_column.error_and_transformation_counts,
        )

        if self.do_bounding_box_filtering:
            df_events = df_events.cache()
            df_events = self.bounding_box_filtering_and_update_qa(
                df_events,
                self.bounding_box,
                self.output_qa_by_column.error_and_transformation_counts,
            )

        if self.do_same_location_deduplication:
            df_events = df_events.cache()
            df_events = self.remove_same_location_duplicates_and_update_qa(
                df_events,
                self.output_qa_by_column.error_and_transformation_counts,
            )

        self.quality_metrics_distribution_after = df_events.groupBy(ColNames.cell_id, ColNames.user_id).agg(
            F.count("*").alias(ColNames.final_frequency)
        )

        # TODO: discuss
        # if we impose the rule on input data that data in a folder
        # is of date specified in folder name - maybe better to use F.lit()
        df_events = df_events.withColumns(
            {
                ColNames.year: F.year(ColNames.timestamp).cast("smallint"),
                ColNames.month: F.month(ColNames.timestamp).cast("tinyint"),
                ColNames.day: F.dayofmonth(ColNames.timestamp).cast("tinyint"),
            }
        )

        # The concept of device demultiplex is implemented here
        # 1) Creates a modulo column, 2) repartitions according to it 3) sorts data within partitions

        df_events = self.calculate_user_id_modulo(df_events, self.number_of_partitions)
        df_events = df_events.repartition(ColNames.user_id_modulo)
        df_events = df_events.sortWithinPartitions(ColNames.user_id, ColNames.timestamp)

        # TODO: remove this, if we decide to save domain column
        df_events = df_events.select(SilverEventDataObject.SCHEMA.fieldNames())

        self.output_data_objects[SilverEventDataObject.ID].df = self.spark.createDataFrame(
            df_events.rdd, SilverEventDataObject.SCHEMA
        )
        # after each chunk processing clear all Cache to free memory and disk
        self.spark.catalog.clearCache()

    def save_syntactic_quality_metrics_frequency_distribution(self):
        """
        Join frequency distribution tables before and after,
        from after table take only final_frequency and replace nulls with 0.
        Create additional column date in DateType(),
        match the schema of SilverEventDataSyntacticQualityMetricsByColumn class
        Write chunk results in separate folders named by processing date
        """

        # Using outer join and groupby after because Spark can not handle joining with null values in columns
        self.output_qa_freq_distribution.df = self.quality_metrics_distribution_before.join(
            self.quality_metrics_distribution_after,
            [ColNames.cell_id, ColNames.user_id],
            "outer",
        ).select(self.quality_metrics_distribution_before.columns + [ColNames.final_frequency])

        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.groupBy(
            ColNames.cell_id, ColNames.user_id
        ).sum(ColNames.initial_frequency, ColNames.final_frequency)
        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.withColumnRenamed(
            f"sum({ColNames.initial_frequency})", ColNames.initial_frequency
        )
        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.withColumnRenamed(
            f"sum({ColNames.final_frequency})", ColNames.final_frequency
        )

        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.fillna(0, ColNames.final_frequency)

        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.withColumn(
            ColNames.date,
            F.to_date(F.lit(self.current_date), self.spark_data_folder_date_format),
        )

        self.output_qa_freq_distribution.df = self.spark.createDataFrame(
            self.output_qa_freq_distribution.df.rdd,
            self.output_qa_freq_distribution.SCHEMA,
        )

        self.output_qa_freq_distribution.write()

    def save_syntactic_quality_metrics_by_column(self):
        """
        Convert output_qa_by_column.error_and_transformation_counts dictionary into spark df.
        Add additional columns, match schema of SilverEventDataSyntacticQualityMetricsByColumn class.
        Write results in default path of this class
        """
        # self.output_qa_by_column( variable, type_of_error, type_of_transformation) : value
        # 3 Nones to match the expected schema
        df_tuples = [
            (variable, type_of_error, type_of_transformation, value)
            for (
                variable,
                type_of_error,
                type_of_transformation,
            ), value in self.output_qa_by_column.error_and_transformation_counts.items()
        ]
        # clear dictionary after each preprocessed day
        self.output_qa_by_column.error_and_transformation_counts.clear()

        temp_schema = StructType(
            [
                StructField(ColNames.variable, StringType(), nullable=True),
                StructField(ColNames.type_of_error, ShortType(), nullable=True),
                StructField(ColNames.type_of_transformation, ShortType(), nullable=True),
                StructField(ColNames.value, IntegerType(), nullable=False),
            ]
        )

        self.output_qa_by_column.df = self.spark.createDataFrame(df_tuples, temp_schema)

        self.output_qa_by_column.df = self.output_qa_by_column.df.withColumns(
            {
                ColNames.result_timestamp: F.lit(F.current_timestamp()),
                ColNames.date: F.to_date(F.lit(self.current_date), self.spark_data_folder_date_format),
            }
        ).select(self.output_qa_by_column.SCHEMA.fieldNames())

        self.output_qa_by_column.df = self.spark.createDataFrame(
            self.output_qa_by_column.df.rdd, self.output_qa_by_column.SCHEMA
        )

        self.output_qa_by_column.write()

    def filter_nulls_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        filter_columns: list[str],
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Loop throuh filter columns (user_id, timestamp)
        delete rows that have null in the corresponfing column.
        Counts the number of filtered records for quality metrics,
        for user_id also calculates the overall correct values,
        since null check is the one and only filter for this column

        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible nulls values
            filter_columns (list[str], optional): columns to check for nulls
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa
        Returns:
            pyspark.sql.dataframe.DataFrame: df without null values in specified columns
        """

        for filter_column in filter_columns:
            filtered_df = df.na.drop(how="any", subset=filter_column)
            error_and_transformation_counts[(filter_column, ErrorTypes.missing_value, None)] += (
                df.count() - filtered_df.count()
            )
            # because timestamp column is then also used in another filters,
            # and no error count should be done in the last filter
            if filter_column not in [ColNames.timestamp, ColNames.mcc]:
                error_and_transformation_counts[(filter_column, ErrorTypes.no_error, None)] += filtered_df.count()

            df = filtered_df.cache()

        return df

    def filter_invalid_mcc_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove any rows where the value for mcc is not a 3 digit number (between 100 and 999)

        Args:
            df (pyspark.sql.dataframe.DataFrame): dataframe of events
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: Dataframe with erroneous values of mcc filtered out
        """

        filtered_df = df.filter(F.col(ColNames.mcc).between(100, 999))
        error_and_transformation_counts[(ColNames.mcc, ErrorTypes.out_of_admissible_values, None)] += (
            df.count() - filtered_df.count()
        )
        # because timestamp column is then also used in another filters,
        # and no error count should be done in the last filter
        error_and_transformation_counts[(ColNames.mcc, ErrorTypes.no_error, None)] += filtered_df.count()

        df = filtered_df.cache()

        return df

    def filter_invalid_mnc_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove any rows where the value for mnc is not a two or three digit
        numerical string

        Args:
            df (pyspark.sql.dataframe.DataFrame): dataframe of events
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: Dataframe with erroneous values of mnc filtered out
        """

        filtered_df = df.filter(
            ((F.length(F.col(ColNames.mnc)) == 2) | (F.length(F.col(ColNames.mnc)) == 3))
            & (F.col(ColNames.mnc).cast("int").isNotNull())
        )

        error_and_transformation_counts[(ColNames.mnc, ErrorTypes.out_of_admissible_values, None)] += (
            df.count() - filtered_df.count()
        )

        error_and_transformation_counts[(ColNames.mnc, ErrorTypes.no_error, None)] += filtered_df.count()

        df = filtered_df.cache()

        return df

    def filter_invalid_plmn_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove any rows where the value for plmn is not a 5 or 6 digit number (between 10000 and 999999)

        Args:
            df (pyspark.sql.dataframe.DataFrame): dataframe of events
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: Dataframe with erroneous values of plmn filtered out
        """

        filtered_df = df.filter(F.col(ColNames.plmn).between(10000, 999999))
        error_and_transformation_counts[(ColNames.plmn, ErrorTypes.out_of_admissible_values, None)] += (
            df.count() - filtered_df.count()
        )

        error_and_transformation_counts[(ColNames.plmn, ErrorTypes.no_error, None)] += filtered_df.count()
        df = filtered_df.cache()

        return df

    def filter_invalid_domain_columns_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Filters out rows that have no domain columns (mcc, mnc, plmn)
        Checks MCC and MNC for domestic and inbound data
        Checks PLMN for outbound data

        Args:
            df (pyspark.sql.dataframe.DataFrame): dataframe of events
            error_and_transformation_counts (_type_): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: Dataframe with erroneous rows removed
        """

        # Filter out columns that have no domain columns
        filtered_df = df.filter(
            (F.col(ColNames.plmn).isNotNull() | ((F.col(ColNames.mcc).isNotNull()) & (F.col(ColNames.mnc).isNotNull())))
        )

        error_and_transformation_counts[(None, ErrorTypes.no_domain, None)] += df.count() - filtered_df.count()

        # Make domain column
        filtered_df = filtered_df.withColumn(
            ColNames.domain,
            F.when(F.col(ColNames.plmn).isNotNull(), ColNames.outbound).otherwise(
                F.when(F.col(ColNames.mcc) == self.local_mcc, ColNames.domestic).otherwise(ColNames.inbound)
            ),
        )

        # For Domestic and inbound check MCC and MNC
        with_mcc_df = filtered_df.filter(F.col(ColNames.domain) != ColNames.outbound)
        with_mcc_df = self.filter_invalid_mcc_and_update_qa(with_mcc_df, error_and_transformation_counts)
        with_mcc_df = self.filter_invalid_mnc_and_update_qa(with_mcc_df, error_and_transformation_counts)

        # Check PLMN for outbound
        outbound_df = filtered_df.filter(F.col(ColNames.domain) == ColNames.outbound)
        outbound_df = self.filter_invalid_plmn_and_update_qa(outbound_df, error_and_transformation_counts)

        final_df = with_mcc_df.union(outbound_df)

        return final_df

    def filter_invalid_cell_id_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove any rows where the value for cell_id is not a 14 or 15 digit number

        Args:
            df (pyspark.sql.dataframe.DataFrame): dataframe of events
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: Dataframe with erroneous values of cell_id filtered out
        """

        filtered_df = df.filter(
            (
                ((F.length(F.col(ColNames.cell_id)) == 14) | (F.length(F.col(ColNames.cell_id)) == 15))
                & (F.col(ColNames.cell_id).cast("long").isNotNull())
                | F.col("cell_id").isNull()
            )
        )
        error_and_transformation_counts[(ColNames.cell_id, ErrorTypes.out_of_admissible_values, None)] += (
            df.count() - filtered_df.count()
        )
        # because timestamp column is then also used in another filters,
        # and no error count should be done in the last filter
        error_and_transformation_counts[(ColNames.cell_id, ErrorTypes.no_error, None)] += filtered_df.count()

        df = filtered_df.cache()

        return df

    def filter_null_locations_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Filter rows with inappropriate location: neither cell_id
        nor latitude&longitude are specified. Count the number of errors,
        since this filter depends on several columns "variable" in quality metrics table will be None.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with possible null location columns
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: filtered df with correctly specified location
        """

        filtered_df = df.filter(
            (F.col(ColNames.cell_id).isNotNull())
            | (F.col(ColNames.longitude).isNotNull() & F.col(ColNames.latitude).isNotNull())
            | (F.col(ColNames.domain) == ColNames.outbound)
        )

        error_and_transformation_counts[(None, ErrorTypes.no_location, None)] += df.count() - filtered_df.count()

        return filtered_df

    def convert_time_column_to_timestamp_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        timestampt_format: str,
        input_timezone: str,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Based on config params timestampt format and input timezone
        convert timestampt column from string to timestampt type, if filter rows with failed conversion.
        Count number of succesful timestampt transformations and number of errors.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with timestampt column
            timestampt_format (str): expected string format to use in time conversion
            input_timezone (str): timezone of the input data
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: df with time column parsed to timestamp
        """

        # TODO: Check timestamp validation
        # Validating timestamp of format '2023-01-03T03:12:00+00:00' raises Exception
        # Error: Fail to parse '2023-01-03T03:12:00+00:00' in the new parser
        # Can we use a conditional(F.when) to check if column can be casted?
        filtered_df = df.withColumn(
            ColNames.timestamp,
            F.to_utc_timestamp(
                F.to_timestamp(ColNames.timestamp, timestampt_format),
                input_timezone,
            ),
        ).filter(F.col(ColNames.timestamp).isNotNull())

        error_and_transformation_counts[
            (ColNames.timestamp, None, Transformations.converted_timestamp)
        ] += filtered_df.count()
        error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.not_right_syntactic_format, None)] += (
            df.count() - filtered_df.count()
        )

        return filtered_df

    def data_period_filter_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        data_period_start: str,
        data_period_end: str,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Filter rows which timestampt is not in specified date range.
        Count the number of error rows for quality metrics,
        and the number of complitely correct timestampt (that pass all corresponding filters)

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with timestamp column
            data_period_start (str): start of date period
            data_period_end (str): end of date period
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa
        Returns:
            pyspark.sql.dataframe.DataFrame: df with records in specified date period
        """
        data_period_start = pd.to_datetime(data_period_start)
        # timedelta is needed to include records happened in data_period_end
        data_period_end = pd.to_datetime(data_period_end) + pd.Timedelta(days=1)
        filtered_df = df.filter(F.col(ColNames.timestamp).between(data_period_start, data_period_end))

        error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.out_of_admissible_values, None)] += (
            df.count() - filtered_df.count()
        )
        # TODO: if we decide not to use data_period_filtering don't forget to put this in convert_time_column_to_timestamp
        error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.no_error, None)] += filtered_df.count()

        return filtered_df

    def bounding_box_filtering_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        bounding_box: dict,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Filter rows which not null longitude & latitude values are
        within coordinates of bounding box. Count the number of errors,
        since this filter depends on several columns "variable" in quality metrics table will be None

        Args:
            df (pyspark.sql.dataframe.DataFrame): df with longitude & latitude columns
            bounding_box (dict): coordinates of bounding box in df_events crs
            error_and_transformation_counts (dict[tuple, int]): dictionary to track qa

        Returns:
            pyspark.sql.dataframe.DataFrame: filtered df with records within bbox
        """
        # coordinates of bounding box should be of the same crs of mno data
        lat_condition = (
            F.col(ColNames.latitude).between(bounding_box["min_lat"], bounding_box["max_lat"])
            | F.col(ColNames.latitude).isNull()
        )
        lon_condition = (
            F.col(ColNames.longitude).between(bounding_box["min_lon"], bounding_box["max_lon"])
            | F.col(ColNames.longitude).isNull()
        )

        filtered_df = df.filter(lat_condition & lon_condition)
        error_and_transformation_counts[(None, ErrorTypes.out_of_bounding_box, None)] += (
            df.count() - filtered_df.count()
        )

        return filtered_df

    def remove_same_location_duplicates_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        error_and_transformation_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove rows that have identical records for
        timestamp, cell_id, longitude, latitude, plmn and user_id.
        Counts the number of removed records for quality metrics.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible duplicates
            duplication_counts (dict[tuple, int]): dictionary to track qa
        Returns:
            pyspark.sql.dataframe.DataFrame: df without duplicate rows
            in terms of user_id, cell_id, latitutde, longitude, plmn
            and timestamp combination
        """

        # if lot and lan are null, they are null for all rows (presumably)
        # the same goes for cell_id

        deduplicated_df = df.drop_duplicates(
            [
                ColNames.user_id,
                ColNames.cell_id,
                ColNames.latitude,
                ColNames.longitude,
                ColNames.timestamp,
                ColNames.plmn,
            ]
        )

        error_and_transformation_counts[(None, ErrorTypes.same_location_duplicate, None)] += (
            df.count() - deduplicated_df.count()
        )

        return deduplicated_df

    def calculate_user_id_modulo(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        modulo_value: int,
        hex_truncation_end: int = 12,
    ) -> pyspark.sql.dataframe.DataFrame:
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
