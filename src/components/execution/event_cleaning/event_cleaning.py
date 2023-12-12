import pyspark.sql.functions as psf
import pyspark

from core.component import Component
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.data_objects.silver.silver_event_data_object import SilverEventDataObject
from core.data_objects.silver.silver_event_data_syntactic_quality_metrics_by_column import SilverEventDataSyntacticQualityMetricsByColumn
from core.data_objects.silver.silver_event_data_syntactic_quality_metrics_frequency_distribution import SilverEventDataSyntacticQualityMetricsFrequencyDistribution
from core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY
from core.columns import ColNames
from core.error_types import ErrorTypes
from core.transformations import Transformations

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, BinaryType, IntegerType, ShortType, DateType
import pandas as pd
import datetime
import os


class EventCleaning(Component):
    COMPONENT_ID = "EventCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.timestamp_format = self.config.get(
            EventCleaning.COMPONENT_ID, 'timestamp_format')
        self.input_timezone = self.config.get(
            EventCleaning.COMPONENT_ID, 'input_timezone')

        self.do_bounding_box_filtering = self.config.getboolean(
            EventCleaning.COMPONENT_ID, 'do_bounding_box_filtering', fallback=False)
        self.bounding_box = self.config.geteval(
            EventCleaning.COMPONENT_ID, 'bounding_box')

    def initalize_data_objects(self):
        # Input
        self.bronze_event_path = self.config.get(
            CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")

        self.data_period_start = self.config.get(
            EventCleaning.COMPONENT_ID, 'data_period_start')
        self.data_period_end = self.config.get(
            EventCleaning.COMPONENT_ID, 'data_period_end')
        self.data_folder_date_format = self.config.get(
            EventCleaning.COMPONENT_ID, 'data_folder_date_format')

        # Create all possible dates between start and end
        sdate = pd.to_datetime(self.data_period_start)
        edate = pd.to_datetime(self.data_period_end)
        self.to_process_dates = list(pd.date_range(
            sdate, edate, freq='d').strftime(self.data_folder_date_format))

        # Create all input data objects
        self.input_event_data_objects = []
        for date in self.to_process_dates:
            path = f"{self.bronze_event_path}/{date}"
            # TODO: think of more elegant solution to check if path exists
            if os.path.exists(path):
                self.input_event_data_objects.append(BronzeEventDataObject(
                    self.spark, path))
            else:
                self.logger.warning(
                    f"Expected path {path} to exist but it does not")
        # Output
        self.output_data_objects = {}

        silver_event_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_data_silver")
        silver_event_do = SilverEventDataObject(self.spark, silver_event_path)
        self.output_data_objects[SilverEventDataObject.ID] = silver_event_do

        event_syntactic_quality_metrics_by_column_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_syntactic_quality_metrics_by_column")
        event_syntactic_quality_metrics_by_column = SilverEventDataSyntacticQualityMetricsByColumn(
            self.spark, event_syntactic_quality_metrics_by_column_path)
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsByColumn.ID] = event_syntactic_quality_metrics_by_column

        self.output_qa_by_column = event_syntactic_quality_metrics_by_column

        # TODO: use this later
        event_syntactic_quality_metrics_frequency_distribution_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_syntactic_quality_metrics_frequency_distribution")
        event_syntactic_quality_metrics_frequency_distribution = SilverEventDataSyntacticQualityMetricsFrequencyDistribution(
            self.spark, event_syntactic_quality_metrics_frequency_distribution_path)
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID] = event_syntactic_quality_metrics_frequency_distribution

    def read(self):
        self.current_input_do.read()

    def write(self):
        self.output_data_objects[SilverEventDataObject.ID].write()
        # TODO: save this
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID].write(self.output_quality_metrics_distribution_path)

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        for input_do in self.input_event_data_objects:
            self.logger.info(f"Reading from path {input_do.default_path}")
            self.current_input_do = input_do
            self.read()
            self.transform()  # Transforms the input_df
            self.write()
        self.save_syntactic_quality_metrics_by_column()
        self.logger.info(f"Finished {self.COMPONENT_ID}")

    def transform(self):

        self.logger.info(f"Transform method {self.COMPONENT_ID}")
        df_events = self.current_input_do.df
        df_events = df_events.cache()
        quality_metrics_distribution_before = df_events.groupBy(ColNames.cell_id, ColNames.user_id)\
                                                       .agg(psf.count("*").alias(ColNames.initial_frequency))
        
        df_events = self.handle_nulls(
            df_events, [ColNames.user_id, ColNames.timestamp])
        
        # already cached in previous function
        df_events = self.convert_time_column_to_timestamp(
            df_events, self.timestamp_format, self.input_timezone)
        
        df_events = df_events.cache()
        # TODO: discuss is it even needed
        df_events = self.data_period_filtering(
            df_events, self.data_period_start, self.data_period_end)

        if self.do_bounding_box_filtering:
            df_events = df_events.cache()
            df_events = self.bounding_box_filtering(
                df_events, self.bounding_box)


        self.spark.catalog.clearCache()

        quality_metrics_distribution_after = df_events.groupBy(ColNames.cell_id, ColNames.user_id)\
                                                      .agg(psf.count("*").alias(ColNames.final_frequency))
        
        quality_metrics_distribution = quality_metrics_distribution_before.join(
            quality_metrics_distribution_after, [ColNames.cell_id, ColNames.user_id], 'left'
        ).select(quality_metrics_distribution_before.columns + [ColNames.final_frequency])

        quality_metrics_distribution = quality_metrics_distribution.fillna(0, ColNames.final_frequency)
        print(self.current_input_do.default_path)
        curr_data = self.current_input_do.default_path.split("/")[-1]
        # TODO: discuss should "yyyyMMdd" go to config? or find format that suites both pandas and spark
        quality_metrics_distribution = quality_metrics_distribution.withColumn(
            ColNames.date, psf.to_date(psf.lit(curr_data), "yyyyMMdd")
        )

        # TODO: discuss
        # if we impose the rule on input data that data in a folder 
        # is of date specified in folder name - maybe better to use F.lit()
        df_events = df_events.withColumns({
            ColNames.year: psf.year(ColNames.timestamp).cast('smallint'),
            ColNames.month: psf.month(ColNames.timestamp).cast('tinyint'),
            ColNames.day: psf.dayofmonth(
                ColNames.timestamp).cast('tinyint')
        })

        df_events = df_events.sort([ColNames.user_id, ColNames.timestamp])

        self.output_data_objects[SilverEventDataObject.ID].df = df_events
        self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID].df = quality_metrics_distribution
        self.output_quality_metrics_distribution_path = f"{self.output_data_objects[SilverEventDataSyntacticQualityMetricsFrequencyDistribution.ID].default_path}/{curr_data}"

    def save_syntactic_quality_metrics_by_column(self):
        # self.output_qa_by_column( variable, type_of_error, type_of_transformation) : value
        # 3 Nones to match the expected schema
        df_tuples = [(variable, type_of_error, type_of_transformation, value) for (
            variable, type_of_error, type_of_transformation), value in self.output_qa_by_column.error_and_transformation_counts.items()]

        temp_schema = StructType([
            StructField(ColNames.variable, StringType(), nullable=True),
            StructField(ColNames.type_of_error, ShortType(), nullable=True),
            StructField(ColNames.type_of_transformation, ShortType(), nullable=True),
            StructField(ColNames.value, IntegerType(), nullable=False),
        ])
        # TODO: use column names from Columns
        self.output_qa_by_column.df = self.spark.createDataFrame(
            df_tuples, temp_schema)

        self.output_qa_by_column.df = self.output_qa_by_column.df\
                                    .withColumns({
                                        ColNames.result_timestamp: psf.lit(psf.current_timestamp()),
                                        ColNames.data_period_start: psf.to_date(psf.lit(self.data_period_start)),
                                        ColNames.data_period_end: psf.to_date(psf.lit(self.data_period_end))
                                    })\
                                    .select(
                                        self.output_qa_by_column.SCHEMA.fieldNames())

        self.output_qa_by_column.df = self.spark.createDataFrame(self.output_qa_by_column.df.rdd,
                                                                 self.output_qa_by_column.SCHEMA)

        self.output_qa_by_column.write()

    def handle_nulls(self,
                     df: pyspark.sql.dataframe.DataFrame,
                     filter_columns: list[str] = None
                     ) -> pyspark.sql.dataframe.DataFrame:

        df = self.filter_nulls(df, filter_columns)
        df = self.filter_null_locations(df)

        return df

    def filter_nulls(self,
                     df: pyspark.sql.dataframe.DataFrame,
                     filter_columns: list[str] = None
                     ) -> pyspark.sql.dataframe.DataFrame:

        for filter_column in filter_columns:

            filtered_df = df.na.drop(how='any', subset=filter_column)
            self.output_qa_by_column.error_and_transformation_counts[(filter_column, ErrorTypes.missing_value, None)] += df.count() - filtered_df.count()
            self.output_qa_by_column.error_and_transformation_counts[(filter_column, ErrorTypes.no_error, None)] += filtered_df.count()

            df = filtered_df.cache()

        return df

    def filter_null_locations(self,
                              df: pyspark.sql.dataframe.DataFrame
                              ) -> pyspark.sql.dataframe.DataFrame:

        filtered_df = df.filter((psf.col(ColNames.cell_id).isNotNull()) |
                       (psf.col(ColNames.longitude).isNotNull() & psf.col(ColNames.latitude).isNotNull()))
        
        self.output_qa_by_column.error_and_transformation_counts[(None, ErrorTypes.no_location, None)] += df.count() - filtered_df.count()

        return filtered_df

    def convert_time_column_to_timestamp(self,
                                         df: pyspark.sql.dataframe.DataFrame,
                                         timestampt_format: str,
                                         input_timezone: str
                                         ) -> pyspark.sql.dataframe.DataFrame:

        filtered_df = df.withColumn(ColNames.timestamp,
                           psf.to_utc_timestamp(psf.to_timestamp(ColNames.timestamp, timestampt_format), input_timezone))\
            .filter(psf.col(ColNames.timestamp).isNotNull())
        
        self.output_qa_by_column.error_and_transformation_counts[(ColNames.timestamp, None, Transformations.converted_timestamp)] += filtered_df.count()
        self.output_qa_by_column.error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.not_right_syntactic_format, None)] += df.count() - filtered_df.count()

        return filtered_df

    def data_period_filtering(self,
                              df: pyspark.sql.dataframe.DataFrame,
                              data_period_start: str,
                              data_period_end: str
                              ) -> pyspark.sql.dataframe.DataFrame:

        data_period_start = pd.to_datetime(data_period_start)
        data_period_end = pd.to_datetime(data_period_end)+pd.Timedelta(days=1)
        filtered_df = df.filter(psf.col(ColNames.timestamp).between(
            data_period_start, data_period_end))
        
        self.output_qa_by_column.error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.out_of_admissable_values, None)] += df.count() - filtered_df.count()
        # TODO: if we do not use data_period_filtering don't forget to put this in convert_time_column_to_timestamp
        self.output_qa_by_column.error_and_transformation_counts[(ColNames.timestamp, ErrorTypes.no_error, None)] += filtered_df.count()

        return filtered_df

    def bounding_box_filtering(self,
                               df: pyspark.sql.dataframe.DataFrame,
                               bounding_box: dict
                               ) -> pyspark.sql.dataframe.DataFrame:
        # coordinates of bounding box should be of the same crs of mno data
        lat_condition = (psf.col(ColNames.latitude).between(
            bounding_box['min_lat'], bounding_box['max_lat']) | psf.col(ColNames.latitude).isNull())
        lon_condition = (psf.col(ColNames.longitude).between(
            bounding_box['min_lon'], bounding_box['max_lon']) | psf.col(ColNames.longitude).isNull())

        filtered_df = df.filter(lat_condition & lon_condition)
        self.output_qa_by_column.error_and_transformation_counts[(None, ErrorTypes.out_of_bounding_box, None)] += df.count() - filtered_df.count()

        return filtered_df
