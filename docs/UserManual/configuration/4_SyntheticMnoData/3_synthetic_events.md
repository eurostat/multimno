---
title: SyntheticEvents Configuration
weight: 3
---

# GeozonesGridMapping Configuration
To initialise and run the component two configs are used - `general_config.ini` and `synthetic_events.ini`. In `general_config.ini` to execute the component specify all paths to its corresponding data objects (input + output). 


```ini
[Paths.Bronze]
diaries_data_bronze = ${Paths:bronze_dir}/diaries
event_data_bronze = ${Paths:bronze_dir}/mno_events
network_data_bronze = ${Paths:bronze_dir}/mno_network
```

In synthetic_events.ini parameters are as follows: 

- **seed** -integer, the random seed value for all subprocesses that involve randomness, such as the random generation of timestamps, latitude, longitude,  random selection of cell to be linked to a point, random selection of rows and columns for null generation, random selection of rows for duplicates generations, etc.

- **event_freq_stays** - integer, the frequency in seconds for events to be generated for stays (higher means that less events will be generated for a given stay in a given time interval in synthetic diaries).

- **event_freq_moves** - integer, the frequency in seconds for events to be generated for moves (higher means that less events will be generated for a given move in a given time interval in synthetic diaries).
  
- **error_location_probability** - float (between 0.0 and 1.0), ratio of rows from all generated events, to be selected for generating errors in x and y coordinates.

- **error_location_distance_min** - integer, the minimum distance in meters to which generated points may reach from the original generated point, when offseting these points to be erroneous

- **error_location_distance_max** - integer, the maximum distance in meters to which generated points may reach from the original generated point, when offseting these points to be erroneous.  

- **error_cell_id_probability** - float (between 0.0 and 1.0), the ratio of rows that are to be selected from all generated events, for generating events with nonexistent cell ids. These are events that have a syntactically valid cell_id that is not present in the input network data.

- **mcc** - integer, the value for the mcc column to be applied to the data of all generated users.

- **maximum_number_of_cells_for_event** - integer, the maximimum number of cells to consider, when linking a generated point to a cell in the input network data. If several cells are within the distance provided by **closest_cell_distance_max** for a given generated point (erroneous or not), a single cell is selected from as many closest cells, randomly, as given by **maximum_number_of_cells_for_event**.

- **closest_cell_distance_max** - integer, the distance in meters to a cell from a generated point, for that cell to be included as one of the possible cells to be linked to the given point.

- **closest_cell_distance_max_for_errors** - integer, the distance in meters to a cell from an erroneously generated point, for the cell to be included as one of the possible cells to be linked to that point.

- **closest_cell_distance_max_for_errors** - integer, the distance in meters to a cell from an erroneously generated point, for the cell to be included as one of the possible cells to be linked to that point.

- **do_event_error_generation** - boolean, whether to generate syntactic errors for clean rows that have been generated.

- **null_row_probability** - float, probability to use for sampling rows for which one or more columns will be set to null. If set to 0, no null rows are generated. Which columns on the sampeld rows are selected as null is affected by the parameter column_is_null_probability.

- **out_of_bounds_probability** - float, probability to use for sampling rows for which timestamp will be replaced with a timestamp that is out of the temporal bounds set in the input synthetic diaries. These rows will not include any other errors.

- **data_type_error_probability** - float, probability to use for sampling rows for which a random selection of columns will be modified as erroneous. Modifications are column specific, for instance the cell_id column will be replaced by random string. 

- **column_is_null_probability** - float, probability that a given column will be set as null for the rows that have been sampled for null selection. If column_is_null_probability=1, all columns in the rows selected for null generation are replaced with nulls. If null_row_probability=0, this parameter is not used.

- **same_location_duplicates_probability** - float, probability for selecting rows that will be transformed into same location duplicates. The value corresponds to the approximate ratio of rows in the final output that will include pair-wise full duplicates. This process generates at a maximum two duplicate records for a given user, not more.

- **different_location_duplicates_probability** - float, probability for selecting rows that will be transformed into different location duplicates. The value corresponds to the approximate ratio of rows in the final output that will include pair-wise duplicates in all columns, except for longitude and latitude. This process generates at a maximum two duplicate records for a given user, not more.

- **order_output_by_timestamp** - boolean, whether to order the final output by the timestamp column.

- **cartesian_crs** - integer, the coordinate reference system (CRS) to use for the cartesian coordinates. The default value is 3035.

**Note on probability parameters**: These parameters do not necessarily translate to the exact ratios for each probability type in the output data object, as the exact number of rows selected is affected by the random seed, in addition to the probability value itself.


## Configuration example

```ini
[Spark]

session_name = SyntheticEventsSession


[SyntheticEvents]
seed = 999
event_freq_stays = 2400 # s
event_freq_moves = 1200  # s
error_location_probability = 0.0
error_location_distance_min = 1000 # m
error_location_distance_max = 10000 # m
error_cell_id_probability  = 0.0
mcc = 214
maximum_number_of_cells_for_event = 3
closest_cell_distance_max = 5000 # m
closest_cell_distance_max_for_errors = 10000 # m
cartesian_crs = 3035

do_event_error_generation = True 
null_row_probability = 0.3 
out_of_bounds_probability = 0.0 
data_type_error_probability = 0.3 
column_is_null_probability = 0.5 
different_location_duplicates_probability = 0.3
same_location_duplicates_probability = 0.25

order_output_by_timestamp = True

```
