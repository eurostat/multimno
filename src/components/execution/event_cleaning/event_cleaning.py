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
        self.mandatory_columns_casting_dict = self.config.geteval(
            EventCleaning.COMPONENT_ID, 'mandatory_columns_casting_dict')
        self.optional_columns_casting_dict = self.config.geteval(
            EventCleaning.COMPONENT_ID, 'optional_columns_casting_dict')

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

    def read(self):
        self.current_input_do.read()

    def write(self):
        self.output_data_objects[SilverEventDataObject.ID].write()
        # TODO: save this
        # self.output_qa_frequency_distribution

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

        # TODO: Create Frequency quality metrics object

        # Parse timestamp to timestamp
        df_events = df_events.withColumn(
            'timestamp', psf.to_timestamp('timestamp'))

        df_events = self.handle_nulls(
            df_events, [ColNames.user_id, ColNames.timestamp])

        df_events = self.convert_time_column_to_timestamp(
            df_events, self.timestamp_format, self.input_timezone)

        df_events = self.data_period_filtering(
            df_events, self.data_period_start, self.data_period_end)

        if self.do_bounding_box_filtering:
            df_events = self.bounding_box_filtering(
                df_events, self.bounding_box)

        df_events = df_events.withColumns({
            ColNames.year: psf.year(ColNames.timestamp).cast('smallint'),
            ColNames.month: psf.month(ColNames.timestamp).cast('tinyint'),
            ColNames.day: psf.dayofmonth(
                ColNames.timestamp).cast('tinyint')
        })

        df_events = df_events.sort([ColNames.user_id, ColNames.timestamp])

        self.output_data_objects[SilverEventDataObject.ID].df = df_events
        # TODO: Assign output_qa_frequency_distribution to self

    def save_syntactic_quality_metrics_by_column(self):
        # self.output_qa_by_column( variable, type_of_error, type_of_transformation) : value
        # 3 Nones to match the expected schema
        df_tuples = [(variable, type_of_error, type_of_transformation, value) for (
            variable, type_of_error, type_of_transformation), value in self.output_qa_by_column.error_and_transformation_counts.items()]

        temp_schema = StructType([
            StructField("variable", StringType(), nullable=True),
            StructField("type_of_error", ShortType(), nullable=True),
            StructField("type_of_transformation", ShortType(), nullable=True),
            StructField("value", IntegerType(), nullable=False),
        ])
        # TODO: use column names from Columns
        self.output_qa_by_column.df = self.spark.createDataFrame(
            df_tuples, temp_schema)

        self.output_qa_by_column.df = self.output_qa_by_column.df.withColumns({
            "result_timestamp": psf.lit(psf.current_timestamp()),
            "data_period_start": psf.to_date(psf.lit("2023-01-01")),
            "data_period_end": psf.to_date(psf.lit("2023-05-01"))
        })

        self.output_qa_by_column.df = self.output_qa_by_column.df.select(
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

        df = df.na.drop(how='any', subset=filter_columns)
        # TODO: update self.output_qa_by_column.error_and_transformation_counts
        # self.output_qa_by_column.error_and_transformation_counts[('cell_id', ErrorTypes.missing_value, None)] += 100
        return df

    def filter_null_locations(self,
                              df: pyspark.sql.dataframe.DataFrame
                              ) -> pyspark.sql.dataframe.DataFrame:

        df = df.filter((psf.col(ColNames.cell_id).isNotNull()) |
                       (psf.col(ColNames.longitude).isNotNull() & psf.col(ColNames.latitude).isNotNull()))

        # TODO: update self.output_qa_by_column.error_and_transformation_counts
        return df

    def convert_time_column_to_timestamp(self,
                                         df: pyspark.sql.dataframe.DataFrame,
                                         timestampt_format: str,
                                         input_timezone: str
                                         ) -> pyspark.sql.dataframe.DataFrame:

        df = df.withColumn(ColNames.timestamp,
                           psf.to_utc_timestamp(psf.to_timestamp(ColNames.timestamp, timestampt_format), input_timezone))\
            .filter(psf.col(ColNames.timestamp).isNotNull())
        # TODO: update self.output_qa_by_column.error_and_transformation_counts
        return df

    def data_period_filtering(self,
                              df: pyspark.sql.dataframe.DataFrame,
                              data_period_start: str,
                              data_period_end: str
                              ) -> pyspark.sql.dataframe.DataFrame:

        data_period_start = pd.to_datetime(data_period_start)
        data_period_end = pd.to_datetime(data_period_end)+pd.Timedelta(days=1)
        df = df.filter(psf.col(ColNames.timestamp).between(
            data_period_start, data_period_end))
        # TODO: update self.output_qa_by_column.error_and_transformation_counts
        return df

    def bounding_box_filtering(self,
                               df: pyspark.sql.dataframe.DataFrame,
                               bounding_box: dict
                               ) -> pyspark.sql.dataframe.DataFrame:
        # coordinates of bounding box should be of the same crs of mno data
        lat_condition = (psf.col(ColNames.latitude).between(
            bounding_box['min_lat'], bounding_box['max_lat']) | psf.col(ColNames.latitude).isNull())
        lon_condition = (psf.col(ColNames.longitude).between(
            bounding_box['min_lon'], bounding_box['max_lon']) | psf.col(ColNames.longitude).isNull())

        df = df.filter(lat_condition & lon_condition)
        # TODO: update self.output_qa_by_column.error_and_transformation_counts
        return df

    def cast_columns(self,
                     df: pyspark.sql.dataframe.DataFrame,
                     mandatory_columns_casting_dict: dict,
                     optional_columns_casting_dict: dict
                     ) -> pyspark.sql.dataframe.DataFrame:

        # for python 3.9 and greater
        columns_casting_dict = mandatory_columns_casting_dict | optional_columns_casting_dict

        # based on optimized logical plan and physical plan spark is smart enough to not cast twice
        for col, dtype in columns_casting_dict.items():
            df = df.withColumn(col, psf.col(col).cast(dtype))

        # nulls in location columns are treated differently
        # optional columns can have null values
        # spark understands that timestamp should not be checked for nulls twice (already done in convert_time_column_to_timestamp function)
        filter_columns = list(set(mandatory_columns_casting_dict.keys())
                              - set([ColNames.cell_id, ColNames.latitude, ColNames.longitude] +
                                    list(optional_columns_casting_dict.keys())))

        df = self.handle_nulls(df, filter_columns)

        return df
