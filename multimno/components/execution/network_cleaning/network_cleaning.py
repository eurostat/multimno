"""
Module that cleans raw MNO Network Topology data.
"""

import datetime
from functools import reduce

from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    DateType,
    StringType,
    StructField,
    StructType,
    ShortType,
    ByteType,
)

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import NetworkErrorType
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_network_data_syntactic_quality_metrics_by_column import (
    SilverNetworkDataQualityMetricsByColumn,
)
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY


class NetworkCleaning(Component):
    """
    Class that cleans MNO Network Topology Data (based on physical properties of the cell)
    """

    COMPONENT_ID = "NetworkCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)
        self.latitude_max = self.config.getfloat(self.COMPONENT_ID, "latitude_max")
        self.latitude_min = self.config.getfloat(self.COMPONENT_ID, "latitude_min")
        self.longitude_max = self.config.getfloat(self.COMPONENT_ID, "longitude_max")
        self.longitude_min = self.config.getfloat(self.COMPONENT_ID, "longitude_min")
        self.tech = ["5G", "LTE", "UMTS", "GSM"]  # hardcoded

        # list of possible cell types
        self.cell_type_options = (
            self.config.get(self.COMPONENT_ID, "cell_type_options").strip().replace(" ", "").split(",")
        )
        self.data_period_format = self.config.get(self.COMPONENT_ID, "data_period_format")

        self.data_period_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), self.data_period_format
        ).date()
        self.data_period_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), self.data_period_format
        ).date()

        # List of datetime.dates for which to perform cleaning of the raw network input data.
        # Notice that date_period_end is included
        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        # Timestamp format that the function pyspark.sql.functions.to_timestamp expects.
        # Format must follow guidelines in https://spark.apache.org/docs/3.4.2/sql-ref-datetime-pattern.html
        self.valid_date_timestamp_format = self.config.get(self.COMPONENT_ID, "valid_date_timestamp_format")

    def initalize_data_objects(self):
        input_bronze_network_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "network_data_bronze")
        output_silver_network_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        output_silver_network_syntactic_quality_metrics_by_column = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_metrics_by_column"
        )

        bronze_network = BronzeNetworkDataObject(
            self.spark, input_bronze_network_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )
        silver_network = SilverNetworkDataObject(
            self.spark, output_silver_network_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        silver_network_quality_metrics_by_column = SilverNetworkDataQualityMetricsByColumn(
            self.spark,
            output_silver_network_syntactic_quality_metrics_by_column,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        self.input_data_objects = {bronze_network.ID: bronze_network}
        self.output_data_objects = {
            silver_network.ID: silver_network,
            silver_network_quality_metrics_by_column.ID: silver_network_quality_metrics_by_column,
        }

    def transform(self):
        # Raw/Bronze Network Topology DF
        cells_df = self.input_data_objects[BronzeNetworkDataObject.ID].df

        # Read only desired dates, specified via config
        cells_df = cells_df.filter(F.make_date("year", "month", "day").isin(self.data_period_dates))

        # List that will contain all the columns created to keep track of every kind of error
        auxiliar_columns = []

        # Columns for which we will check for null values
        # Notice that currently, valid_date_end can have null values by definition as long as for the current date the tower is
        # still operational. Thus, it is not taken into account for the deletion of rows/records
        check_for_null_columns = [
            ColNames.cell_id,
            ColNames.valid_date_start,
            ColNames.valid_date_end,  # should not be counted for discarding rows!!
            ColNames.latitude,
            ColNames.longitude,
            ColNames.altitude,
            ColNames.antenna_height,
            ColNames.directionality,
            ColNames.elevation_angle,
            ColNames.horizontal_beam_width,
            ColNames.vertical_beam_width,
            ColNames.power,
            ColNames.frequency,
            ColNames.technology,
            ColNames.cell_type,
        ]

        # Add auxiliar columns to track instances where a row has a null value
        # Note that currently valid_date_end has a permited null value, as well as
        # azimith_angle when directionality is 0
        cells_df = cells_df.withColumns(
            {f"{col}_{NetworkErrorType.NULL_VALUE}": F.col(col).isNull() for col in check_for_null_columns}
        ).withColumn(
            f"{ColNames.azimuth_angle}_{NetworkErrorType.NULL_VALUE}",
            F.when(F.col(ColNames.directionality) == F.lit(1), F.col(ColNames.azimuth_angle).isNull()).otherwise(
                F.lit(False)
            ),
        )

        auxiliar_columns.extend([f"{col}_{NetworkErrorType.NULL_VALUE}" for col in check_for_null_columns])
        auxiliar_columns.append(f"{ColNames.azimuth_angle}_{NetworkErrorType.NULL_VALUE}")

        # Now, we try to parse the valid_date_start and valid_date_end columns, from a string to a timestamp
        cells_df = (
            cells_df.withColumn(
                f"{ColNames.valid_date_start}_parsed",
                F.to_timestamp(F.col(ColNames.valid_date_start), self.valid_date_timestamp_format),
            )
            .withColumn(
                f"{ColNames.valid_date_end}_parsed",
                F.to_timestamp(F.col(ColNames.valid_date_end), self.valid_date_timestamp_format),
            )
            # Check when parsing failed, excluding the cases where the field was null to begi with
            .withColumn(
                f"{ColNames.valid_date_start}_{NetworkErrorType.CANNOT_PARSE}",
                F.when(
                    F.col(ColNames.valid_date_start).isNotNull()
                    & F.col(f"{ColNames.valid_date_start}_parsed").isNull(),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                f"{ColNames.valid_date_end}_{NetworkErrorType.CANNOT_PARSE}",
                F.when(
                    F.col(ColNames.valid_date_end).isNotNull() & F.col(f"{ColNames.valid_date_end}_parsed").isNull(),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
        )

        auxiliar_columns.extend(
            [
                f"{ColNames.valid_date_start}_{NetworkErrorType.CANNOT_PARSE}",
                f"{ColNames.valid_date_end}_{NetworkErrorType.CANNOT_PARSE}",
            ]
        )

        # Now, we check for incoherent dates (valid_date_end is earlier in time than valid_date_start)
        cells_df = cells_df.withColumn(
            f"dates_{NetworkErrorType.OUT_OF_RANGE}",
            F.when(
                F.col(f"{ColNames.valid_date_start}_parsed").isNotNull()
                & F.col(f"{ColNames.valid_date_end}_parsed").isNotNull(),
                F.col(f"{ColNames.valid_date_start}_parsed") > F.col(f"{ColNames.valid_date_end}_parsed"),
            ).otherwise(F.lit(False)),
        )

        auxiliar_columns.append(f"dates_{NetworkErrorType.OUT_OF_RANGE}")

        # Now we check for invalid values that are outside of the range defined for the data object.

        # TODO: correct check for CGI in cell ids
        cells_df = cells_df.withColumn(
            f"{ColNames.cell_id}_{NetworkErrorType.OUT_OF_RANGE}",
            (F.length(F.col(ColNames.cell_id)) != F.lit(14)) & (F.length(F.col(ColNames.cell_id)) != F.lit(15)),
        )

        # TODO: cover case where bounding box crosses the -180/180 longitude
        cells_df = (
            cells_df.withColumn(
                f"{ColNames.latitude}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.latitude) > F.lit(self.latitude_max))
                | (F.lit(ColNames.latitude) < F.lit(self.latitude_min)),
            )
            .withColumn(
                f"{ColNames.longitude}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.longitude) > F.lit(self.longitude_max))
                | (F.lit(ColNames.longitude) < F.lit(self.longitude_min)),
            )
            # altitude: must only be float, no checks
            # antenna height: must be positive
            .withColumn(
                f"{ColNames.antenna_height}_{NetworkErrorType.OUT_OF_RANGE}",
                F.col(ColNames.antenna_height) <= F.lit(0),
            )
            # directionality: 0 or 1
            .withColumn(
                f"{ColNames.directionality}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.directionality) != F.lit(0)) & (F.col(ColNames.directionality) != F.lit(1)),
            )
            .withColumn(
                f"{ColNames.azimuth_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                F.when(
                    (F.col(ColNames.directionality) == F.lit(1)),  # & F.col(ColNames.azimuth_angle).isNotNull(),
                    (F.col(ColNames.azimuth_angle) < F.lit(0)) | (F.col(ColNames.azimuth_angle) > F.lit(360)),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                f"{ColNames.elevation_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.elevation_angle) < F.lit(-90)) | (F.col(ColNames.elevation_angle) > F.lit(90)),
            )
            .withColumn(
                f"{ColNames.horizontal_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.horizontal_beam_width) < F.lit(0))
                | (F.col(ColNames.horizontal_beam_width) > F.lit(360)),
            )
            .withColumn(
                f"{ColNames.vertical_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                (F.col(ColNames.vertical_beam_width) < F.lit(0)) | (F.col(ColNames.vertical_beam_width) > F.lit(360)),
            )
            .withColumn(
                f"{ColNames.power}_{NetworkErrorType.OUT_OF_RANGE}",
                F.col(ColNames.power) < F.lit(0),
            )
            .withColumn(
                f"{ColNames.frequency}_{NetworkErrorType.OUT_OF_RANGE}",
                F.col(ColNames.frequency) < F.lit(0),
            )
            .withColumn(
                f"{ColNames.technology}_{NetworkErrorType.OUT_OF_RANGE}",
                ~F.col(ColNames.technology).isin(self.tech),
            )
            .withColumn(
                f"{ColNames.cell_type}_{NetworkErrorType.OUT_OF_RANGE}",
                ~F.col(ColNames.cell_type).isin(self.cell_type_options),
            )
        )

        # Null values will appear for the above checks when the raw data was null. Thus, for these columns
        # we change null for False by using the .fillna() method
        cells_df = cells_df.fillna(
            False,
            [
                f"{ColNames.cell_id}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.latitude}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.longitude}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.antenna_height}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.directionality}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.azimuth_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.elevation_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.horizontal_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.vertical_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.power}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.frequency}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.technology}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.cell_type}_{NetworkErrorType.OUT_OF_RANGE}",
            ],
        )

        auxiliar_columns.extend(
            [
                f"{ColNames.cell_id}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.latitude}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.longitude}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.antenna_height}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.directionality}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.azimuth_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.elevation_angle}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.horizontal_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.vertical_beam_width}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.power}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.frequency}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.technology}_{NetworkErrorType.OUT_OF_RANGE}",
                f"{ColNames.cell_type}_{NetworkErrorType.OUT_OF_RANGE}",
            ]
        )

        # Auxiliar dict, relating the DO columns with its auxiliar columns
        column_groups = dict()
        for col in self.output_data_objects["SilverNetworkDO"].SCHEMA.names:
            # for each auxiliar column
            column_groups[col] = []
            for cc in auxiliar_columns:
                # if it is related to the DO's column:
                if col == "_".join(cc.split("_")[:-1]):
                    # Ignore nulls for valid date end
                    if cc == f"{ColNames.valid_date_end}_{NetworkErrorType.NULL_VALUE}":
                        continue
                    column_groups[col].append(F.col(cc))

        # For each column, create an abstract conditional whenever a value of that column has ANY type of error.
        # Example: latitude can have two types of error: a) being null, or b) being out of range.
        # The coniditonal for this column is then> (isNull(latitude) OR isOutOfRange(latitude))
        column_conditions = {
            col: reduce(lambda a, b: a | b, column_groups[col]) for col in column_groups if len(column_groups[col]) > 0
        }

        # Negate the conditionals above to get those records without ANY type of error
        field_without_errors = {col: ~column_conditions[col] for col in column_conditions}

        auxiliar_columns.extend([f"{col}_{NetworkErrorType.NO_ERROR}" for col in field_without_errors])

        # Abstract conditional ,indicating those records that do not have any type of error in any column, i.e. all accepted
        # values.
        preserve_row = reduce(lambda a, b: a & b, field_without_errors.values())

        cells_df = cells_df.withColumns(
            {f"{col}_{NetworkErrorType.NO_ERROR}": field_without_errors[col] for col in field_without_errors}
        )

        cells_df = cells_df.withColumn("to_preserve", preserve_row)

        cells_df.cache()

        # Collect the number of True values in each auxiliar column, that counts the number of each error type, or
        # any error, in each of the columns of the data object.
        # TODO: possible improvement if pyspark.sql.GroupedData.pivot can be used instead.
        metrics = (
            cells_df.withColumns({col: F.col(col).cast(IntegerType()) for col in auxiliar_columns})
            .groupBy([F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)])
            .agg({col: "sum" for col in auxiliar_columns})
            .withColumnsRenamed({f"sum({col})": col for col in auxiliar_columns})
            .collect()
        )

        # Extract the collected values and reformat them into the shape of the metrics DO dataframe.
        metrics_long_format = []

        for row in metrics:
            row = row.asDict()
            year = row[ColNames.year]
            month = row[ColNames.month]
            day = row[ColNames.day]
            date = datetime.date(year=year, month=month, day=day)
            row_dict = dict()

            for col in row:
                if col in [ColNames.year, ColNames.month, ColNames.day]:
                    continue

                row_dict = {
                    ColNames.field_name: "_".join(col.split("_")[:-1]),
                    ColNames.type_code: int(col.split("_")[-1]),
                    ColNames.value: row[col],
                    ColNames.date: date,
                    ColNames.year: year,
                    ColNames.month: month,
                    ColNames.day: day,
                }

                metrics_long_format.append(Row(**row_dict))

        # Initial records (before cleaning)
        initial_records = (
            cells_df.groupBy([F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)]).count().collect()
        )

        for row in initial_records:
            row = row.asDict()
            year = row[ColNames.year]
            month = row[ColNames.month]
            day = row[ColNames.day]
            date = datetime.date(year=year, month=month, day=day)

            row_dict = {
                ColNames.field_name: None,
                ColNames.type_code: NetworkErrorType.INITIAL_ROWS,
                ColNames.value: row["count"],
                ColNames.date: date,
                ColNames.year: year,
                ColNames.month: month,
                ColNames.day: day,
            }

            metrics_long_format.append(Row(**row_dict))

        # Final records (after cleaning)
        final_records = (
            cells_df.withColumn("to_preserve", F.col("to_preserve").cast(IntegerType()))
            .groupBy([F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)])
            .sum("to_preserve")
            .withColumnRenamed("sum(to_preserve)", "count")
            .collect()
        )
        for row in final_records:
            row = row.asDict()
            year = row[ColNames.year]
            month = row[ColNames.month]
            day = row[ColNames.day]
            date = datetime.date(year=year, month=month, day=day)

            row_dict = {
                ColNames.field_name: None,
                ColNames.type_code: NetworkErrorType.FINAL_ROWS,
                ColNames.value: row["count"],
                ColNames.date: date,
                ColNames.year: year,
                ColNames.month: month,
                ColNames.day: day,
            }

            metrics_long_format.append(Row(**row_dict))

        # Final result
        metrics_df = self.spark.createDataFrame(
            metrics_long_format,
            schema=StructType(
                [
                    StructField(ColNames.field_name, StringType(), nullable=True),
                    StructField(ColNames.type_code, IntegerType(), nullable=False),
                    StructField(ColNames.value, IntegerType(), nullable=False),
                    StructField(ColNames.date, DateType(), nullable=False),
                    StructField(ColNames.year, ShortType(), nullable=False),
                    StructField(ColNames.month, ByteType(), nullable=False),
                    StructField(ColNames.day, ByteType(), nullable=False),
                ]
            ),
        )
        metrics_df = metrics_df.withColumn(ColNames.result_timestamp, F.current_timestamp())

        self.output_data_objects[SilverNetworkDataQualityMetricsByColumn.ID].df = metrics_df

        silver_cells_df = (
            cells_df.filter(F.col("to_preserve"))
            .withColumn(ColNames.valid_date_start, F.col(f"{ColNames.valid_date_start}_parsed"))
            .withColumn(ColNames.valid_date_end, F.col(f"{ColNames.valid_date_end}_parsed"))
            .select(SilverNetworkDataObject.SCHEMA.names)
        )

        self.output_data_objects[SilverNetworkDataObject.ID].df = silver_cells_df
