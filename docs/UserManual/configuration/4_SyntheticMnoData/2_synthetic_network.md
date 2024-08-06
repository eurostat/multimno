---
title: SyntheticNetwork Configuration
weight: 2
---

# LongtermPermanenceScore Configuration
To initialise and run the component two configs are used - `general_config.ini` and `synthetic_network.ini`. In `general_config.ini` all paths to the corresponding data objects shall be specified. Example:

```ini
[Paths.Bronze]
...
network_data_bronze = ${Paths:bronze_dir}/mno_network
...
```

The expected parameters in `synthetic_network.ini` are as follows:
 - **seed**: integer, seed for random number generation used to generate the synthetic network topology data.
 - **n_cells**: positive integer, number of synthetic cells that will be generated. Example: `500`.
 - **cell_id_generation_type**: string, identifier of the generator of cell IDs to be used. Currently the only option available is `random_cell_id`, which generates a random 14- or 15- digit string. Example: `random_cell_id`.
 - **cell_type_options**: comma-separated list of strings, it contains the values that the `cell_type` field can take for the generated synthetic data. Each option has the same probability of being assigned. Example: `macrocell, microcell, picocell, femtocell`.
 - **latitude_min**: float, minimum latitude that defines the bounding box in which the coordinates of the synthetic cells will be generated.
 - **latitude_max**: float, maximum latitude that defines the bounding box in which the coordinates of the synthetic cells will be generated.
 - **longitude_min**: float, minimum longitude that defines the bounding box in which the coordinates of the synthetic cells will be generated.
 - **longitude_max**: float, maximum longitude that defines the bounding box in which the coordinates of the synthetic cells will be generated.
 - **altitude_min**: float, minimum value that the altitude field can take in the generated cells. Example: `-40`.
 - **altitude_max**: float, maximum value that the altitude field can take in the generated cells. Example: `5000`.
 - **antenna_height_max**: float, maximum value that the `antenna_height` field might take in the generated cells. Example: `120`.
 - **altitude_min**: float, minimum value that the altitude field can take in the generated cells. Example: `-40`.
 - **altitude_max**: float, maximum value that the altitude field can take in the generated cells. Example: `5000`.
 - **power_min**: float, minimum value that the power field can take in the generated cells. Units are watts (W). Example: `0.1`.
 - **power_max**: float, maximum value that the power field can take in the generated cells. Units are watts (W). Example: `500`.
 - **frequency_min**: float, minimum value that the frequency field can take in the generated cells. Units are megahertz (MHz). Example: `1`.
 - **frequency_max**: float, maximum value that the frequency field can take in the generated cells. Units are megahertz (MHz). Example: `4000`.
 - **data_period_format**: string, it indicates the format expected in `earliest_valid_date_start` and `latest_valid_date_end`. For example, use `%Y-%m-%dT%H:%M:%S` for the usual "2023-01-09T00:00:00" format.
 - **earliest_valid_date_start**: string, it indicates the timestamp value that the `valid_date_start` will take in all generated cells. Example: `2022-12-15T00:00:00`.
 - **latest_valid_date_end**: string, it indicates the timestamp value that the `valid_date_end` will take in all generated cells. Example: `2023-01-15T00:00:00`.
 - **date_format**: string, it indicates the format expected in `starting_date` and `ending_date`. For example, use `%Y-%m-%d` for the usual "2023-01-09" format separated by `-`. Example: `%Y-%m-%d`.
 - **starting_date**: string, format should be the one specified `date_format` (e.g., `2023-01-01` for `%Y-%m-%d`), the first date for which data will be generated. All dates between this one and the specified in `ending_date` will be have data generated (both inclusive). The cell properties will be equal across all dates. 
 - **ending_date**: string, format should be the one specified `date_format` (e.g., `2023-01-09` for `%Y-%m-%d`), the last date for which data will be generated. All dates between the specified in `starting_date` and this one will be have data generated (both inclusive). The cell properties will be equal across all dates. 
 - **no_optional_fields_probability**: float, probability that all of the optional fields of a record take the null value. Example: `0.05`.
 - **mandatory_null_probability**: float, probability that one of the mandatory fields of a record will take a null value. Example: `0.05`.
 - **out_of_bounds_values_probability**: float, probability that a field of a record will take a value outside its valid values. This could be, for example, a negative power or a latitude outside the $[-90, 90]$ interval. Example: `0.05`.
 - **erroneous_values_probability**: float, probability that one of the following erroneous values might occur:
   - The `cell_id` takes a non-valid value (not a 14- or 15-digit string).
   - The `valid_date_start` and `valid_date_end` fields has an invalid timestamp format.
   - The `valid_date_end` is a point in time earlier than `valid_date_start`.

    Example: `0.05`.


## Configuration example

```ini
[Spark]
session_name = SyntheticNetworkSession

[SyntheticNetwork]
seed = 33
n_cells = 500
cell_id_generation_type = random_cell_id  # options: random_cell_id
cell_type_options = macrocell, microcell, picocell, femtocell
latitude_min = 40.352
latitude_max = 40.486
longitude_min = -3.751
longitude_max = -3.579

altitude_min = -40
altitude_max = 5000
# antenna_height_min is always 0
antenna_height_max = 120
power_min = 0.1
power_max = 500
frequency_min = 1
frequency_max = 4000
timestamp_format = %Y-%m-%dT%H:%M:%S
earliest_valid_date_start = 2022-12-15T00:00:00
latest_valid_date_end = 2023-01-15T00:00:00
date_format = %Y-%m-%d
starting_date = 2023-01-01
ending_date = 2023-01-09

no_optional_fields_probability = 0.0
mandatory_null_probability = 0.0
out_of_bounds_values_probability = 0.0
erroneous_values_probability = 0.0
```