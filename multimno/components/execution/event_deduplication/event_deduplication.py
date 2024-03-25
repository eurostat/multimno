"""
Module that cleans RAW MNO Event data.
"""

import pandas as pd
import pyspark
import pyspark.sql.functions as psf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType
from pyspark.sql import Window

from multimno.core.component import Component
from multimno.core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import (
    SilverEventDataSyntacticQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import (
    SilverEventDataSyntacticQualityMetricsFrequencyDistribution,
)
from multimno.core.spark_session import check_if_data_path_exists, delete_file_or_folder
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import ErrorTypes


class EventDeduplication(Component):
    """
    Class that removes duplicates from clean MNO Event data
    """

    COMPONENT_ID = "EventDeduplication"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.spark_data_folder_date_format = self.config.get(
            EventDeduplication.COMPONENT_ID, "spark_data_folder_date_format"
        )

    def initalize_data_objects(self):
        # Input
        self.input_silver_events_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver")

        self.data_period_start = self.config.get(EventDeduplication.COMPONENT_ID, "data_period_start")
        self.data_period_end = self.config.get(EventDeduplication.COMPONENT_ID, "data_period_end")
        self.data_folder_date_format = self.config.get(EventDeduplication.COMPONENT_ID, "data_folder_date_format")
        self.clear_destination_directory = self.config.get(
            EventDeduplication.COMPONENT_ID, "clear_destination_directory"
        )

        # Create all possible dates between start and end
        sdate = pd.to_datetime(self.data_period_start)
        edate = pd.to_datetime(self.data_period_end)
        self.to_process_dates = list(pd.date_range(sdate, edate, freq="d"))

        # Create all input data objects
        self.input_event_data_objects = {SilverEventDataObject.ID: None}
        path = self.input_silver_events_path

        if check_if_data_path_exists(self.spark, path):
            # Single input option, with a filter follwed by it
            self.input_event_data_objects[SilverEventDataObject.ID] = SilverEventDataObject(
                self.spark, self.input_silver_events_path
            )

        else:
            self.logger.warning(f"Expected path {path} to exist but it does not")

        # Output data objects dictionary
        self.output_data_objects = {}

        deduplicated_silver_events_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver_deduplicated")
        silver_event_do = SilverEventDataObject(self.spark, deduplicated_silver_events_path)
        self.output_data_objects[SilverEventDataObject.ID] = silver_event_do

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, silver_event_do.default_path)

        event_deduplicated_quality_metrics_by_column_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_deduplicated_quality_metrics_by_column"
        )
        event_deduplicated_quality_metrics_by_column = SilverEventDataSyntacticQualityMetricsByColumn(
            self.spark, event_deduplicated_quality_metrics_by_column_path
        )
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID] = (
            event_deduplicated_quality_metrics_by_column
        )

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, event_deduplicated_quality_metrics_by_column.default_path)

        self.output_qa_by_column = event_deduplicated_quality_metrics_by_column

        event_deduplicated_quality_metrics_frequency_distribution_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_deduplicated_quality_metrics_frequency_distribution"
        )
        event_deduplicated_quality_metrics_frequency_distribution = (
            SilverEventDataSyntacticQualityMetricsFrequencyDistribution(
                self.spark, event_deduplicated_quality_metrics_frequency_distribution_path
            )
        )
        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, event_deduplicated_quality_metrics_frequency_distribution.default_path)

        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID] = (
            event_deduplicated_quality_metrics_frequency_distribution
        )

        # this instance of SilverEventDataSyntacticQualityMetricsFrequencyDistribution class
        # will be used to write frequency distrobution of each preprocessing date (chunk)
        # the path argument will be changed dynamically

        self.output_qa_freq_distribution = event_deduplicated_quality_metrics_frequency_distribution

    def read(self):
        self.input_event_data_objects[SilverEventDataObject.ID].read()
        # self.read()

    def write(self):
        self.output_data_objects[SilverEventDataObject.ID].write()
        self.save_deduplicated_quality_metrics_frequency_distribution()

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")

        self.read()

        # for current_date in self.dates_to_process:
        for current_date in self.to_process_dates:
            self.current_date = current_date

            self.current_input_do = self.input_event_data_objects[SilverEventDataObject.ID].df.filter(
                (
                    (psf.col(ColNames.year) == current_date.year)
                    & (psf.col(ColNames.month) == current_date.month)
                    & (psf.col(ColNames.day) == current_date.day)
                )
            )

            self.transform()

            if self.output_qa_by_column.error_and_transformation_counts is not None:
                self.save_deduplicated_quality_metrics_by_column()

            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        df_events = self.current_input_do

        # cache before each filter function because we appply action count()
        df_events = df_events.cache()

        self.quality_metrics_distribution_before = (
            df_events.groupBy(ColNames.cell_id, ColNames.user_id, ColNames.user_id_modulo)
            .agg(psf.count("*").alias(ColNames.initial_frequency))
            .drop(ColNames.user_id_modulo)
        )

        # Perform duplicate removal

        df_events = self.remove_same_location_duplicates_and_update_qa(
            df_events,
            self.output_qa_by_column.error_and_transformation_counts,
        )

        df_events = self.remove_different_location_duplicates_and_update_qa(
            df_events,
            self.output_qa_by_column.error_and_transformation_counts,
        )

        df_events = df_events.withColumn(
            ColNames.date, psf.to_date(psf.col(ColNames.timestamp), self.spark_data_folder_date_format)
        )

        self.quality_metrics_distribution_after = (
            df_events.groupBy(ColNames.cell_id, ColNames.user_id, ColNames.user_id_modulo, ColNames.date)
            .agg(psf.count("*").alias(ColNames.final_frequency))
            .drop(ColNames.user_id_modulo)
        )

        df_events = df_events.drop(ColNames.date)  # dropping internal date used for prior calculation of metrics

        # TODO: currently results in several files per one partition when commented out
        # Improve this part after performance tests

        # df_events = df_events.repartition(ColNames.user_id_modulo)
        # df_events = df_events.sortWithinPartitions(ColNames.user_id_modulo, ColNames.user_id, ColNames.timestamp)

        self.output_data_objects[SilverEventDataObject.ID].df = self.spark.createDataFrame(
            df_events.rdd, SilverEventDataObject.SCHEMA
        )

        # after each chunk processing clear all Cache to free memory and disk
        self.spark.catalog.clearCache()

    def save_deduplicated_quality_metrics_frequency_distribution(self):
        """
        Join frequency distribution tables before and after,
        from after table take only final_frequency and replace nulls with 0.
        Create additional column date in DateType(),
        match the schema of SilverEventDataDeduplicationQualityMetricsByColumn class
        Write chunk results in separate folders named by processing date
        """

        self.output_qa_freq_distribution.df = self.quality_metrics_distribution_before.join(
            self.quality_metrics_distribution_after, [ColNames.cell_id, ColNames.user_id], "left"
        ).select(self.quality_metrics_distribution_before.columns + [ColNames.final_frequency])

        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.fillna(0, ColNames.final_frequency)

        self.output_qa_freq_distribution.df = self.output_qa_freq_distribution.df.withColumn(
            ColNames.date, psf.to_date(psf.lit(self.current_date), self.spark_data_folder_date_format)
        )

        self.output_qa_freq_distribution.df = self.spark.createDataFrame(
            self.output_qa_freq_distribution.df.rdd, self.output_qa_freq_distribution.SCHEMA
        )

        self.output_qa_freq_distribution.write()

    def save_deduplicated_quality_metrics_by_column(self):
        """
        Convert output_qa_by_column.error_and_transformation_counts dictionary into spark df.
        Add additional columns, match schema of SilverEventDataDeduplicationQualityMetricsByColumn class.
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
                ColNames.result_timestamp: psf.lit(psf.current_timestamp()),
                ColNames.date: psf.to_date(psf.lit(self.current_date), self.spark_data_folder_date_format),
            }
        ).select(self.output_qa_by_column.SCHEMA.fieldNames())

        self.output_qa_by_column.df = self.spark.createDataFrame(
            self.output_qa_by_column.df.rdd, self.output_qa_by_column.SCHEMA
        )

        self.output_qa_by_column.write()

    def remove_same_location_duplicates_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        duplication_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove rows that have identical records for
        timestamp, cell_id, longitude, latitude and user_id.
        Counts the number of removed records for quality metrics.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible duplicates
            duplication_counts (dict[tuple, int]): dictionary to track qa
        Returns:
            pyspark.sql.dataframe.DataFrame: df without duplicate rows
            in terms of user_id, cell_id, latitutde, longitude
            and timestamp combination
        """

        # if lot and lan are null, they are null for all rows (presumably)
        # the same goes for cell_id

        deduplicated_df = df.drop_duplicates(
            [ColNames.user_id, ColNames.cell_id, ColNames.latitude, ColNames.longitude, ColNames.timestamp]
        )

        duplication_counts[(None, ErrorTypes.same_location_duplicate, None)] += df.count() - deduplicated_df.count()

        return deduplicated_df

    def remove_different_location_duplicates_and_update_qa(
        self,
        df: pyspark.sql.dataframe.DataFrame,
        duplication_counts: dict[tuple:int],
    ) -> pyspark.sql.dataframe.DataFrame:
        """
        Remove rows that have the same timestamp for a user.
        Requires input where same location duplicates have been removed.
        In case a user has two or more identical records for a given timestamp,
        all of such records are removed. The duplication check is performed
        over two columns: timestamp and user_id.

        Args:
            df (pyspark.sql.dataframe.DataFrame): df events with possible duplicates for
                different locations, but no duplicates for same location
                duplication_counts (dict[tuple, int]): dictionary to track qa
        Returns:
            pyspark.sql.dataframe.DataFrame: df without duplicate rows
                in terms of user_id and timestamp combination
        """

        window_dedupl = Window.partitionBy(*[ColNames.user_id_modulo, ColNames.user_id, ColNames.timestamp])

        deduplicated_df = df.withColumn("count", psf.count("*").over(window_dedupl)).where("count=1").drop("count")

        duplication_counts[(None, ErrorTypes.different_location_duplicate, None)] += (
            df.count() - deduplicated_df.count()
        )

        return deduplicated_df
