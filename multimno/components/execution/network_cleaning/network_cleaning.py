"""
Module that cleans raw MNO Network Topology data.
"""

import datetime
from functools import reduce

from pyspark.sql import Row, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import (
    IntegerType,
    DateType,
    StringType,
    StructField,
    StructType,
    ShortType,
    ByteType,
    FloatType,
    TimestampType,
)

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import NetworkErrorType
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.core.data_objects.silver.silver_network_data_object import SilverNetworkDataObject
from multimno.core.data_objects.silver.silver_network_data_syntactic_quality_metrics_by_column import (
    SilverNetworkDataQualityMetricsByColumn,
)
from multimno.core.data_objects.silver.silver_network_data_top_frequent_errors_data_object import (
    SilverNetworkDataTopFrequentErrors,
)
from multimno.core.data_objects.silver.silver_network_row_error_metrics import SilverNetworkRowErrorMetrics
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY, CONFIG_SILVER_PATHS_KEY
from multimno.core.log import get_execution_stats


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

        # list of possible technologies
        self.tech = self.config.get(self.COMPONENT_ID, "technology_options").strip().replace(" ", "").split(",")

        # list of possible cell types
        self.cell_type_options = (
            self.config.get(self.COMPONENT_ID, "cell_type_options").strip().replace(" ", "").split(",")
        )

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

        # Timestamp format that the function pyspark.sql.functions.to_timestamp expects.
        # Format must follow guidelines in https://spark.apache.org/docs/3.4.2/sql-ref-datetime-pattern.html
        self.valid_date_timestamp_format = self.config.get(self.COMPONENT_ID, "valid_date_timestamp_format")

        self.frequent_error_criterion = self.config.get(self.COMPONENT_ID, "frequent_error_criterion")
        if self.frequent_error_criterion not in ("absolute", "percentage"):
            raise ValueError(
                "unexpected value in frequent_error_criterion: expected `absolute` or `percentage`, got",
                self.frequent_error_criterion,
            )

        if self.frequent_error_criterion == "absolute":
            self.top_k_errors = self.config.getint(self.COMPONENT_ID, "top_k_errors")
        else:  # percentage
            self.top_k_errors = self.config.getfloat(self.COMPONENT_ID, "top_k_errors")

        self.timestamp = datetime.datetime.now()
        self.current_date: datetime.date = None
        self.cells_df: DataFrame = None
        self.accdf: DataFrame = None

    def initalize_data_objects(self):
        input_bronze_network_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "network_data_bronze")
        output_silver_network_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        output_silver_network_syntactic_quality_metrics_by_column = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_syntactic_quality_metrics_by_column"
        )
        output_silver_network_top_errors_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_top_frequent_errors")
        output_silver_network_row_error_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "network_row_error_metrics"
        )

        bronze_network = BronzeNetworkDataObject(self.spark, input_bronze_network_path)
        silver_network = SilverNetworkDataObject(self.spark, output_silver_network_path)

        silver_network_quality_metrics_by_column = SilverNetworkDataQualityMetricsByColumn(
            self.spark,
            output_silver_network_syntactic_quality_metrics_by_column,
        )

        silver_network_row_error_metrics = SilverNetworkRowErrorMetrics(
            self.spark,
            output_silver_network_row_error_metrics_path,
        )

        silver_network_top_errors = SilverNetworkDataTopFrequentErrors(
            self.spark,
            output_silver_network_top_errors_path,
        )

        self.input_data_objects = {bronze_network.ID: bronze_network}
        self.output_data_objects = {
            silver_network.ID: silver_network,
            silver_network_quality_metrics_by_column.ID: silver_network_quality_metrics_by_column,
            silver_network_top_errors.ID: silver_network_top_errors,
            silver_network_row_error_metrics.ID: silver_network_row_error_metrics,
        }

    def transform(self):
        # Raw/Bronze Network Topology DF
        self.cells_df = self.input_data_objects[BronzeNetworkDataObject.ID].df

        # Read only desired dates, specified via config
        self.cells_df = self.cells_df.filter(
            F.make_date(ColNames.year, ColNames.month, ColNames.day) == F.lit(self.current_date)
        )

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
            ColNames.range,
            ColNames.frequency,
            ColNames.technology,
            ColNames.cell_type,
        ]

        # Add auxiliar columns to track instances where a row has a null value
        # Note that currently valid_date_end has a permited null value, as well as
        # azimith_angle when directionality is 0
        self.cells_df = self.cells_df.withColumns(
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
        self.cells_df = (
            self.cells_df.withColumn(
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
        self.cells_df = self.cells_df.withColumn(
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
        self.cells_df = self.cells_df.withColumn(
            f"{ColNames.cell_id}_{NetworkErrorType.OUT_OF_RANGE}",
            (F.length(F.col(ColNames.cell_id)) != F.lit(14)) & (F.length(F.col(ColNames.cell_id)) != F.lit(15)),
        )

        # TODO: cover case where bounding box crosses the -180/180 longitude
        self.cells_df = (
            self.cells_df.withColumn(
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
                f"{ColNames.range}_{NetworkErrorType.OUT_OF_RANGE}",
                F.col(ColNames.range) < F.lit(0),
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
        self.cells_df = self.cells_df.fillna(
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
                f"{ColNames.range}_{NetworkErrorType.OUT_OF_RANGE}",
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
                f"{ColNames.range}_{NetworkErrorType.OUT_OF_RANGE}",
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

        # Abstract conditional ,indicating those records that do not have any type of error in any
        # mandatory column, i.e. all accepted values.
        mandatory_columns = [
            ColNames.cell_id,
            ColNames.latitude,
            ColNames.longitude,
            ColNames.directionality,
            ColNames.azimuth_angle,
        ]

        # Rows to be preserved are those without errors in their mandatory fields
        preserve_row = reduce(lambda a, b: a & b, [field_without_errors[col] for col in mandatory_columns])

        # Rows to be deleted

        # Rows with any type of error
        any_error_row = reduce(lambda a, b: a | b, column_conditions.values())

        self.cells_df = self.cells_df.withColumns(
            {f"{col}_{NetworkErrorType.NO_ERROR}": field_without_errors[col] for col in field_without_errors}
        )

        self.cells_df = self.cells_df.withColumn("to_preserve", preserve_row)

        self.cells_df.cache()

        rows_to_be_deleted = (
            self.cells_df.select((~preserve_row).cast(ByteType()).alias("to_be_deleted")).withColumn(
                "to_be_deleted", F.sum("to_be_deleted")
            )
        ).collect()[0]["to_be_deleted"]

        rows_with_any_error = (
            self.cells_df.select((any_error_row).cast(ByteType()).alias("row_with_some_error")).withColumn(
                "row_with_some_error", F.sum("row_with_some_error")
            )
        ).collect()[0]["row_with_some_error"]

        row_error_metrics = []
        row_error_metrics.append(
            Row(
                **{
                    ColNames.result_timestamp: self.timestamp,
                    ColNames.variable: "rows_deleted",
                    ColNames.value: rows_to_be_deleted,
                    ColNames.year: self.current_date.year,
                    ColNames.month: self.current_date.month,
                    ColNames.day: self.current_date.day,
                }
            )
        )

        row_error_metrics.append(
            Row(
                **{
                    ColNames.result_timestamp: self.timestamp,
                    ColNames.variable: "rows_with_some_error",
                    ColNames.value: rows_with_any_error,
                    ColNames.year: self.current_date.year,
                    ColNames.month: self.current_date.month,
                    ColNames.day: self.current_date.day,
                }
            )
        )

        row_error_df = self.spark.createDataFrame(row_error_metrics, schema=SilverNetworkRowErrorMetrics.SCHEMA)

        self.output_data_objects[SilverNetworkRowErrorMetrics.ID].df = row_error_df

        # Collect the number of True values in each auxiliar column, that counts the number of each error type, or
        # any error, in each of the columns of the data object.
        # TODO: possible improvement if pyspark.sql.GroupedData.pivot can be used instead.
        metrics = (
            self.cells_df.withColumns({col: F.col(col).cast(IntegerType()) for col in auxiliar_columns})
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
            self.cells_df.groupBy([F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)]).count().collect()
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
            self.cells_df.withColumn("to_preserve", F.col("to_preserve").cast(IntegerType()))
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
        metrics_df = metrics_df.withColumn(ColNames.result_timestamp, F.lit(self.timestamp).cast(TimestampType()))

        self.output_data_objects[SilverNetworkDataQualityMetricsByColumn.ID].df = metrics_df

        silver_cells_df = self.cells_df.filter(F.col("to_preserve"))
        # Prepare the clean, silver network data by imputing null values in invalid optional fields
        for auxcol in auxiliar_columns:
            # do not impute fields without error or that are already null
            if int(auxcol.split("_")[-1]) not in (NetworkErrorType.NO_ERROR, NetworkErrorType.NULL_VALUE):
                split_col = auxcol.split("_")
                variable = "_".join(split_col[:-1])

                if variable == "dates":
                    silver_cells_df = silver_cells_df.withColumn(
                        ColNames.valid_date_start,
                        F.when(F.col(auxcol), F.lit(None)).otherwise(F.col(ColNames.valid_date_start)),
                    ).withColumn(
                        ColNames.valid_date_end,
                        F.when(F.col(auxcol), F.lit(None)).otherwise(F.col(ColNames.valid_date_end)),
                    )
                    continue

                silver_cells_df = silver_cells_df.withColumn(
                    variable, F.when(F.col(auxcol), F.lit(None)).otherwise(F.col(variable))
                )

        silver_cells_df = (
            silver_cells_df.withColumn(ColNames.valid_date_start, F.col(f"{ColNames.valid_date_start}_parsed"))
            .withColumn(ColNames.valid_date_end, F.col(f"{ColNames.valid_date_end}_parsed"))
            .select(SilverNetworkDataObject.SCHEMA.names)
        )

        self.output_data_objects[SilverNetworkDataObject.ID].df = silver_cells_df

        # Top Frequent Error Metrics
        error_counts_df = []
        for field_name, cols in column_groups.items():
            if len(cols) == 0:
                continue

            # Get name of the column and its error code
            col_name = cols[0]._jc.toString()
            type_code_column = F.when(F.col(col_name), F.lit(int(col_name.split("_")[-1])))

            # if len(cols) == 1, the loop is not entered
            for i in range(1, len(cols)):
                col_name = cols[i]._jc.toString()
                type_code_column = type_code_column.when(F.col(col_name), F.lit(int(col_name.split("_")[-1])))
            type_code_column = type_code_column.otherwise(None)

            error_counts_df.append(
                self.cells_df.filter(~field_without_errors[field_name])  # rows with errors in this field
                .select(field_name, *cols)  # select field and its aux columns
                .withColumn(ColNames.type_code, type_code_column)  # new column with error code
                .groupBy(field_name, ColNames.type_code)
                .count()  # count frequency of each particular error value
                .withColumnsRenamed(
                    {
                        "count": ColNames.error_count,
                        field_name: ColNames.error_value,
                    }
                )
                .withColumn(ColNames.error_count, F.col(ColNames.error_count).cast(IntegerType()))
                .withColumn(  # cast values as strings
                    ColNames.error_value, F.col(ColNames.error_value).cast(StringType())
                )
                .withColumn(ColNames.field_name, F.lit(field_name))
            )

        # Join all error count dataframes
        errors_df = reduce(lambda x, y: DataFrame.union(x, y), error_counts_df)

        errors_df.cache()

        total_errors = errors_df.select(F.sum(ColNames.error_count).alias(ColNames.error_count)).collect()[0][
            ColNames.error_count
        ]

        if total_errors is None:
            self.output_data_objects[SilverNetworkDataTopFrequentErrors.ID].df = self.spark.createDataFrame(
                [], schema=SilverNetworkDataTopFrequentErrors.SCHEMA
            )
            return

        window = (
            Window()
            .orderBy(F.col(ColNames.error_count).desc())
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        self.accdf = errors_df.withColumn(
            ColNames.accumulated_percentage,
            (F.lit(100 / total_errors) * F.sum(F.col(ColNames.error_count)).over(window)).cast(FloatType()),
        ).withColumn("id", F.row_number().over(window))

        if self.frequent_error_criterion == "absolute":
            self.accdf = self.accdf.filter(F.col("id") <= F.lit(self.top_k_errors)).drop("id")
        else:  # percentage
            self.accdf.cache()
            prev_id = (
                self.accdf.filter(F.col(ColNames.accumulated_percentage) <= F.lit(self.top_k_errors)).select(
                    F.max("id")
                )
            ).collect()[0]["max(id)"]

            if prev_id is None:
                prev_id = 0

            self.accdf = self.accdf.filter(F.col("id") <= F.lit(prev_id + 1)).drop("id")

        self.accdf = (
            self.accdf.withColumn(ColNames.result_timestamp, F.lit(self.timestamp).cast(TimestampType()))
            .withColumn(ColNames.year, F.lit(self.current_date.year).cast(ShortType()))
            .withColumn(ColNames.month, F.lit(self.current_date.month).cast(ByteType()))
            .withColumn(ColNames.day, F.lit(self.current_date.day).cast(ByteType()))
            .select(SilverNetworkDataTopFrequentErrors.SCHEMA.fieldNames())
        )

        self.output_data_objects[SilverNetworkDataTopFrequentErrors.ID].df = self.accdf

    @get_execution_stats
    def execute(self):
        self.read()
        for date in self.data_period_dates:
            self.logger.info(f"Processing {date}...")
            self.current_date = date
            self.accdf = None
            self.transform()
            self.write()
            self.cells_df.unpersist()
            if self.accdf is not None:
                self.accdf.unpersist()
            else:
                self.logger.info(f"No errors found for {date} -- no error frequency metrics generated")
            self.logger.info(f"... {date} finished")

    def write(self):
        """
        Method that performs the write operation of the output data objects.
        """
        for data_object in self.output_data_objects.values():
            self.logger.info(f"Writing {data_object.ID}...")
            data_object.write()
            self.logger.info("... finished")

        for data_object in self.output_data_objects.values():
            data_object.df.unpersist()
