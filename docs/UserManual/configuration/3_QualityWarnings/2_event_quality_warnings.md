---
title: EventQualityWarnings Configuration
weight: 2
---

# EventQualityWarnings Configuration
To initialise and run the component two configs are used -  `general_config.ini` and component’s config (either  `event_cleaning_quality_warnings.ini` or  `event_deduplication_quality_warnings.ini`). In  `general_config.ini` to execute Event Quality Warnings component specify all paths to its corresponding data objects. Example with specified paths for both cases:


```ini
[Paths.Silver]
...
# for Event Cleaning Quality Warnings
event_syntactic_quality_metrics_by_column = ${Paths:silver_dir}/event_syntactic_quality_metrics_by_column
event_syntactic_quality_metrics_frequency_distribution = ${Paths:silver_dir}/event_syntactic_quality_metrics_frequency_distribution
event_syntactic_quality_warnings_log_table = ${Paths:silver_dir}/event_syntactic_quality_warnings_log_table
event_syntactic_quality_warnings_for_plots = ${Paths:silver_dir}/event_syntactic_quality_warnings_for_plots

```

Below there is a description of one of sub component’s config  - `event_cleaning_quality_warnings.ini`. 

Parameters are as follows:

Under `[EventQualityWarnings]` config section: 

- **input_qm_by_column_path_key** - string, key in Paths.Silver section in general config for a path to corresponding Quality Metrics By Column data

- **input_qm_freq_distr_path_key** - string, key in Paths.Silver section in general config for a path to corresponding Quality Metrics Frequency Distribution data

- **output_qw_log_table_path_key** - string, key in Paths.Silver section in general config for a path to write output Quality Warnings Log Table

- **output_qw_for_plots_path_key** - string, key in Paths.Silver section in general config for a path to write output Quality Warnings ForPLots

- **data_period_start** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-08), the date from which start Event Quality Warnings, by now make sure the first day(s) of research period has enough previous data in in Quality Metrics Frequency Fistribution and Quality Metrics By Column 

- **data_period_end** - string, format should be “yyyy-MM-dd“ (e.g. 2023-01-15), the date till which perform Event Quality Warnings

- **lookback_period** - the length of lookback period, represented as string (could be either ‘week' or 'month') which than will get its numeric representation in number of days

- **do_size_raw_data_qw** - boolean, whether perform QW checks on `initial_frequency` column in Quality Metrics Frequency Fistribution

- **do_size_clean_data_qw** - boolean, whether perform QW checks on `final_frequency` column in Quality Metrics Frequency Distribution

- **data_size_tresholds** - dictionary, with keys as thresholds names and values as corresponding thresholds values for  `do_size_raw_data_qw` and `do_size_clean_data_qw`

- **do_error_rate_by_date_qw** - boolean, whether to perform QW checks on total error rate by `date`

- **do_error_rate_by_date_and_cell_qw** -  boolean, whether to perform QW checks on total error rate by `date` and `cell_id`

- **do_error_rate_by_date_and_user_qw** -  boolean, whether to perform QW checks on total error rate by `date` and `user_id`

- **do_error_rate_by_date_and_cell_user_qw** - boolean, whether to perform QW checks on total error rate by `date`, `cell_id` and `user_id`

- **error_rate_tresholds** - dictionary, with keys as thresholds names and values as corresponding thresholds values for  `do_error_rate_by_date_qw`, `do_error_rate_by_date_and_cell_qw`, `do_error_rate_by_date_and_user_qw`, and `do_error_rate_by_date_and_cell_user_qw`

- **error_type_qw_checks** - dictionary, where the keys are names of error types (please see `multimno/core/constants/error_types.py` file) and values list of column names on which you want to perform QWs of the this error type. Example: during Event Cleaning three columns are checked for null values, if you want to check error rate of `missing_value` type for all mentioned columns specify them in the list. Some error types might have None for column names, which means that technically this kind or error do not belong to just one column but several (e.g. for `no_location` error three columns are used - cell_id, lat, lon): 

```ini
error_type_qw_checks = {
    'missing_value': ['user_id', 'mcc', 'timestamp'],
    'out_of_bounding_box':[None]
    }
```
If you do not intend to run QWs on some error type leave its corresponding list of columns empty.

- **thresholds for each error_type & column combination you want to compute QWs**  - thresholds are combined in groups: each set of thresholds relevant to some error type is a separate config param of type dictionary, where keys are column names, values is another dictionary of structure: `threshold_name:threshold_value`. Example: 

```ini
missing_value_thresholds = {
    'user_id': {"Missing_value_RATE_BYDATE_USER_AVERAGE": 30,
                "Missing_value_RATE_BYDATE_USER_VARIABILITY": 2,
                "Missing_value_RATE_BYDATE_USER_ABS_VALUE_UPPER_LIMIT": 20
               }, 
    'mcc': {"Missing_value_RATE_BYDATE_MCC_AVERAGE": 30,
            "Missing_value_RATE_BYDATE_MCC_VARIABILITY": 2,
            "Missing_value_RATE_BYDATE_MCC_ABS_VALUE_UPPER_LIMIT": 20
           }, 
    'timestamp': {"Missing_value_RATE_BYDATE_TIMESTAMP_AVERAGE": 30,
                  "Missing_value_RATE_BYDATE_TIMESTAMP_VARIABILITY": 2,
                  "Missing_value_RATE_BYDATE_TIMESTAMP_ABS_VALUE_UPPER_LIMIT": 20
                 }
    }
out_of_bounding_box_thresholds = {
    None: {"Out_of_bbox_RATE_BYDATE_AVERAGE": 30,
           "Out_of_bbox_RATE_BYDATE_VARIABILITY": 2,
           "Out_of_bbox_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20
          }
    }
```
Make sure, that for each column of interest specified in `error_type_qw_checks` there are corresponding thresholds. The order of thresholds is important and should be: `AVERAGE`, `VARIABILITY`, and `ABS_VALUE_UPPER_LIMIT` (at least by now all error type QWs follow the same logic and thus their computation is done within one function with ordered threshold arguments). Currently the code supports running QWs on following thresholds: 

```ini
# possible thresholds in event_cleaning_quality_warnings.ini
missing_value_thresholds

out_of_admissible_values_thresholds

not_right_syntactic_format_thresholds

no_location_thresholds

no_domain_thresholds

out_of_bounding_box_thresholds

deduplication_same_location_thresholds
```
- **clear_destination_directory** - boolean, if True deletes all output of the Component in init stage


## Configuration example

```ini
[EventQualityWarnings]
# keys in Paths.Silver section in general config
input_qm_by_column_path_key = event_syntactic_quality_metrics_by_column
input_qm_freq_distr_path_key = event_syntactic_quality_metrics_frequency_distribution
output_qw_log_table_path_key = event_syntactic_quality_warnings_log_table
output_qw_for_plots_path_key = event_syntactic_quality_warnings_for_plots
# BY NOW make sure that the first day(s) of research period has enough previous data
# of df_qa_by_column and df_qa_freq_distribution 
# (e.g. staring from 2023-01-01, if period is a week and start period is 2023-01-08)
data_period_start = 2023-01-01
# you can exceed max(df_qa_by_column.date) 
# although you will still get QWs for dates till max(df_qa_by_column.date), including
data_period_end = 2023-01-09
# should be either week or month
lookback_period = week
# SIZE QA
do_size_raw_data_qw = True
do_size_clean_data_qw = True
data_size_tresholds = {
    "SIZE_RAW_DATA_BYDATE_VARIABILITY": 3,
    "SIZE_RAW_DATA_BYDATE_ABS_VALUE_UPPER_LIMIT": 10000000,
    "SIZE_RAW_DATA_BYDATE_ABS_VALUE_LOWER_LIMIT": 0,
    "SIZE_CLEAN_DATA_BYDATE_VARIABILITY": 3,
    "SIZE_CLEAN_DATA_BYDATE_ABS_VALUE_UPPER_LIMIT": 10000000,
    "SIZE_CLEAN_DATA_BYDATE_ABS_VALUE_LOWER_LIMIT": 0,
    }
# ERROR RATE QW
do_error_rate_by_date_qw = True
do_error_rate_by_date_and_cell_qw = False
do_error_rate_by_date_and_user_qw = True
do_error_rate_by_date_and_cell_user_qw = True
error_rate_tresholds = {
    "TOTAL_ERROR_RATE_BYDATE_OVER_AVERAGE": 30,
    "TOTAL_ERROR_RATE_BYDATE_VARIABILITY": 2,
    "TOTAL_ERROR_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20,
    "ERROR_RATE_BYDATE_BYCELL_OVER_AVERAGE": 30,
    "ERROR_RATE_BYDATE_BYCELL_VARIABILITY": 2,
    "ERROR_RATE_BYDATE_BYCELL_ABS_VALUE_UPPER_LIMIT": 20,
    "ERROR_RATE_BYDATE_BYUSER_OVER_AVERAGE": 30,
    "ERROR_RATE_BYDATE_BYUSER_VARIABILITY": 2,
    "ERROR_RATE_BYDATE_BYUSER_ABS_VALUE_UPPER_LIMIT": 20,
    "ERROR_RATE_BYDATE_BYCELL_USER_OVER_AVERAGE": 30,
    "ERROR_RATE_BYDATE_BYCELL_USER_VARIABILITY": 2,
    "ERROR_RATE_BYDATE_BYCELL_USER_ABS_VALUE_UPPER_LIMIT": 20,
    }
# ERROR TYPE QW
# for each type of error (key), specified the colums you want to check, naming of columns must be oidentical to ColNames
# if you do not want to run qw on some error_type leave the list empty
# None - for no_location and out_of_bounding_box because they do not have more than one column used for this error_type
# for more clarity please check event_cleaning.py
error_type_qw_checks = {
    'missing_value': ['user_id', 'mcc', 'timestamp'],
    'out_of_admissible_values': ['cell_id', 'mcc', 'mnc', 'plmn', 'timestamp'],
    'not_right_syntactic_format': ['timestamp'], 
    'no_domain': [None],
    'no_location':[None], 
    'out_of_bounding_box':[None],
    'same_location_duplicate':[None]
    }
# for each dict_error_type_thresholds make sure you specified all relevant columns
# the order of thresholds is important, should be: AVERAGE, VARIABILITY, and ABS_VALUE_UPPER_LIMIT
missing_value_thresholds = {
    'user_id': {"Missing_value_RATE_BYDATE_USER_AVERAGE": 30,
                "Missing_value_RATE_BYDATE_USER_VARIABILITY": 2,
                "Missing_value_RATE_BYDATE_USER_ABS_VALUE_UPPER_LIMIT": 20
               }, 
    'mcc': {"Missing_value_RATE_BYDATE_MCC_AVERAGE": 30,
            "Missing_value_RATE_BYDATE_MCC_VARIABILITY": 2,
            "Missing_value_RATE_BYDATE_MCC_ABS_VALUE_UPPER_LIMIT": 20
           },
    'mnc': {"Missing_value_RATE_BYDATE_MNC_AVERAGE": 30,
            "Missing_value_RATE_BYDATE_MNC_VARIABILITY": 2,
            "Missing_value_RATE_BYDATE_MNC_ABS_VALUE_UPPER_LIMIT": 20
           }, 
    'plmn': {"Missing_value_RATE_BYDATE_PLMN_AVERAGE": 30,
            "Missing_value_RATE_BYDATE_PLMN_VARIABILITY": 2,
            "Missing_value_RATE_BYDATE_PLMN_ABS_VALUE_UPPER_LIMIT": 20
           }, 
    'timestamp': {"Missing_value_RATE_BYDATE_TIMESTAMP_AVERAGE": 30,
                  "Missing_value_RATE_BYDATE_TIMESTAMP_VARIABILITY": 2,
                  "Missing_value_RATE_BYDATE_TIMESTAMP_ABS_VALUE_UPPER_LIMIT": 20
                 }
    }
out_of_admissible_values_thresholds = {
    'cell_id': {"Out_of_range_RATE_BYDATE_CELL_AVERAGE": 30,
                "Out_of_range_RATE_BYDATE_CELL_VARIABILITY": 2,
                "Out_of_range_RATE_BYDATE_CELL_ABS_VALUE_UPPER_LIMIT": 20
               }, 
    'mcc': {"Out_of_range_RATE_BYDATE_MCC_AVERAGE": 30,
            "Out_of_range_RATE_BYDATE_MCC_VARIABILITY": 2,
            "Out_of_range_RATE_BYDATE_MCC_ABS_VALUE_UPPER_LIMIT": 20
           }, 
    'mnc': {"Out_of_range_RATE_BYDATE_MNC_AVERAGE": 30,
            "Out_of_range_RATE_BYDATE_MNC_VARIABILITY": 2,
            "Out_of_range_RATE_BYDATE_MNC_ABS_VALUE_UPPER_LIMIT": 20
           },
    'plmn': {"Out_of_range_RATE_BYDATE_PLMN_AVERAGE": 30,
            "Out_of_range_RATE_BYDATE_PLMN_VARIABILITY": 2,
            "Out_of_range_RATE_BYDATE_PLMN_ABS_VALUE_UPPER_LIMIT": 20
           },
    'timestamp': {"Out_of_range_RATE_BYDATE_TIMESTAMP_AVERAGE": 30,
                  "Out_of_range_RATE_BYDATE_TIMESTAMP_VARIABILITY": 2,
                  "Out_of_range_RATE_BYDATE_TIMESTAMP_ABS_VALUE_UPPER_LIMIT": 20
                 }
    }
not_right_syntactic_format_thresholds = {
    'timestamp': {"Wrong_type_RATE_BYDATE_TIMESTAMP_AVERAGE": 30,
                  "Wrong_type_RATE_BYDATE_TIMESTAMP_VARIABILITY": 2,
                  "Wrong_type_RATE_BYDATE_TIMESTAMP_ABS_VALUE_UPPER_LIMIT": 20
                 }
    }
no_location_thresholds = {
    None: {"No_location_RATE_BYDATE_AVERAGE": 30,
           "No_location_RATE_BYDATE_VARIABILITY": 2,
           "No_location_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20
          }
    }
no_domain_thresholds = {
    None: {"No_domain_RATE_BYDATE_AVERAGE": 30,
           "No_domain_RATE_BYDATE_VARIABILITY": 2,
           "No_domain_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20
          }
    }
out_of_bounding_box_thresholds = {
    None: {"Out_of_bbox_RATE_BYDATE_AVERAGE": 30,
           "Out_of_bbox_RATE_BYDATE_VARIABILITY": 2,
           "Out_of_bbox_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20
          }
    }
deduplication_same_location_thresholds = {
    None: {"Deduplication_same_location_RATE_BYDATE_AVERAGE": 30,
           "Deduplication_same_location_RATE_BYDATE_VARIABILITY": 2,
           "Deduplication_same_location_RATE_BYDATE_ABS_VALUE_UPPER_LIMIT": 20
          }
    }

clear_destination_directory = True
```