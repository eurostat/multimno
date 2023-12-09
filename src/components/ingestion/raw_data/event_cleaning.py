from core.component import Component
from core.data_objects.landing.landing_event_data_object import LandingEventDataObject
from core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from core.settings import CONFIG_LANDING_PATHS_KEY
from core.settings import CONFIG_BRONZE_PATHS_KEY
from core.columns import ColNames

import pyspark
import pyspark.sql.functions as F

class EventCleaning(Component):
    COMPONENT_ID = "EventCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)

        self.timestamp_format = self.config.get(EventCleaning.COMPONENT_ID, 'timestamp_format')
        self.input_timezone = self.config.get(EventCleaning.COMPONENT_ID, 'input_timezone')
        self.data_period_start = self.config.get(EventCleaning.COMPONENT_ID, 'data_period_start')
        self.data_period_end = self.config.get(EventCleaning.COMPONENT_ID, 'data_period_end')
        self.do_bounding_box_filtering = self.config.getboolean(EventCleaning.COMPONENT_ID, 'do_bounding_box_filtering', fallback=False)
        self.bounding_box = self.config.geteval(EventCleaning.COMPONENT_ID, 'bounding_box')
        self.mandatory_columns_casting_dict = self.config.geteval(EventCleaning.COMPONENT_ID, 'mandatory_columns_casting_dict')
        self.optional_columns_casting_dict = self.config.geteval(EventCleaning.COMPONENT_ID, 'optional_columns_casting_dict')


    def initalize_data_objects(self):
        # Input
        landing_event_path = self.config.get(
            CONFIG_LANDING_PATHS_KEY, "event_data_landing")
        landing_event_do = LandingEventDataObject(self.spark, landing_event_path)
        self.input_data_objects = {
            LandingEventDataObject.ID: landing_event_do
        }

        # Output
        bronze_event_path = self.config.get(
            CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        bronze_event_do = BronzeEventDataObject(self.spark, bronze_event_path)
        self.output_data_objects = {
            BronzeEventDataObject.ID: bronze_event_do
        }

    def read(self):
        self.logger.info(f"Read method {self.COMPONENT_ID}")
        do = self.input_data_objects[LandingEventDataObject.ID]
        do.read()

    def transform(self):
        self.logger.info(f"Transform method {self.COMPONENT_ID}")

        df = self.input_data_objects[LandingEventDataObject.ID].df

        df = self.check_existance_of_columns(df, 
                                             list(self.mandatory_columns_casting_dict.keys()), 
                                             self.optional_columns_casting_dict)
        
        df = self.handle_nulls(df, [ColNames.user_id, ColNames.timestamp])

        df = self.convert_time_column_to_timestamp(df,self.timestamp_format,self.input_timezone)

        df = self.data_period_filtering(df,self.data_period_start,self.data_period_end)

        if self.do_bounding_box_filtering:
            df = self.bounding_box_filtering(df,self.bounding_box)
        
        df = self.cast_columns(df,self.mandatory_columns_casting_dict,self.optional_columns_casting_dict)

        df = df.withColumns({
            ColNames.year: F.year(ColNames.timestamp).cast('smallint'), 
            ColNames.month: F.month(ColNames.timestamp).cast('tinyint'), 
            ColNames.day: F.dayofmonth(ColNames.timestamp).cast('tinyint')
        })

        df = df.sort([ColNames.user_id, ColNames.timestamp])

        # should be there any schema check? 
        self.output_data_objects[BronzeEventDataObject.ID].df = df

    def write(self):
        # copied from event_synthetic
        self.logger.info(f"Write method {self.COMPONENT_ID}")

        bronze_event_do: BronzeEventDataObject = self.output_data_objects[BronzeEventDataObject.ID]
        sdate = self.config.get(EventCleaning.COMPONENT_ID, 'dates_string')
        path = f"{bronze_event_do.default_path}/{sdate}"
        # partition columns should be configurable ?
        bronze_event_do.write(path, [ColNames.year, ColNames.month, ColNames.day])
  

    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()
        self.transform()
        self.write()
        self.logger.info(f"Finished {self.COMPONENT_ID}")


    def check_existance_of_columns(self,
                                   df: pyspark.sql.dataframe.DataFrame, 
                                   mandatory_columns: list[str], 
                                   optional_columns_casting_dict: dict[str:str]
                                  ) -> pyspark.sql.dataframe.DataFrame:
        
        df_columns = df.columns
        optional_columns = list(optional_columns_casting_dict.keys())

        mandatory_common_columns = set(mandatory_columns).intersection(set(df_columns))
        optional_common_columns = set(optional_columns).intersection(set(df_columns))

        if len(mandatory_common_columns) != len(mandatory_columns):
            raise KeyError("Not all mandatory columns in df are present")

        missing_optional_columns = set(optional_columns) - set(optional_common_columns)
        for missing_optional_column in missing_optional_columns:
            df = df.withColumn(missing_optional_column, 
                               F.lit(None).cast(optional_columns_casting_dict[missing_optional_column]))
            
        df = df.select(mandatory_columns + optional_columns)
        
        return df
    
    def handle_nulls(self, 
                     df: pyspark.sql.dataframe.DataFrame, 
                     filter_columns: list[str]= None
                    ) -> pyspark.sql.dataframe.DataFrame:
        
        df = self.filter_nulls(df, filter_columns)
        df = self.filter_null_locations(df)

        return df
    
    def filter_nulls(self, 
                     df: pyspark.sql.dataframe.DataFrame, 
                     filter_columns: list[str]= None
                    ) -> pyspark.sql.dataframe.DataFrame:
        
        df = df.na.drop(how='any', subset = filter_columns)
    
        return df
    

    def filter_null_locations(self, 
                              df: pyspark.sql.dataframe.DataFrame
                             ) -> pyspark.sql.dataframe.DataFrame:
    
        df = df.filter((F.col(ColNames.cell_id).isNotNull()) | 
                       (F.col(ColNames.longitude).isNotNull()&F.col(ColNames.longitude).isNotNull()))

        return df
    

    def convert_time_column_to_timestamp(self, 
                                         df: pyspark.sql.dataframe.DataFrame, 
                                         timestampt_format: str, 
                                         input_timezone: str
                                        ) -> pyspark.sql.dataframe.DataFrame:

        df = df.withColumn(ColNames.timestamp,  
                           F.to_utc_timestamp(F.to_timestamp(ColNames.timestamp, timestampt_format), input_timezone))\
               .filter(F.col(ColNames.timestamp).isNotNull())

        return df
    
    def data_period_filtering(self,
                              df: pyspark.sql.dataframe.DataFrame, 
                              data_period_start: str, 
                              data_period_end: str
                            ) -> pyspark.sql.dataframe.DataFrame:
    
        data_period_start = F.to_date(F.lit(data_period_start))
        data_period_end = F.to_date(F.lit(data_period_end))
        # inclusive on both sides
        df = df.filter(F.col(ColNames.timestamp).between(data_period_start, data_period_end))

        return df
    
    def bounding_box_filtering(self, 
                               df: pyspark.sql.dataframe.DataFrame,
                               bounding_box: dict
                              ) -> pyspark.sql.dataframe.DataFrame:
        # coordinates of bounding box should be of the same crs of mno data
        lat_condition = (F.col(ColNames.latitude).between(bounding_box['min_lat'], bounding_box['max_lat']))
        lon_condition = (F.col(ColNames.longitude).between(bounding_box['min_lon'], bounding_box['max_lon']))

        df = df.filter(lat_condition & lon_condition)

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
            df = df.withColumn(col, F.col(col).cast(dtype))
            
        # nulls in location columns are treated differently
        # optional columns can have null values
        # spark understands that timestamp should not be checked for nulls twice (already done in convert_time_column_to_timestamp function)
        filter_columns = list(set(mandatory_columns_casting_dict.keys())\
                            - set([ColNames.cell_id, ColNames.latitude, ColNames.longitude] +\
                                   list(optional_columns_casting_dict.keys())))

        df = self.handle_nulls(df, filter_columns)

        return df