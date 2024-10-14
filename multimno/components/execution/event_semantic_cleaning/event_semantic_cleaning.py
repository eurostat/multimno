"""
Module that computes semantic checks on event data and adds error flags
"""

import datetime

from pyspark.sql import Window, DataFrame, Row
from pyspark.sql.types import LongType, ShortType, ByteType, StructField, StructType
import pyspark.sql.functions as F
import sedona.sql.st_functions as STF
import sedona.sql.st_constructors as STC

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import SemanticErrorType
from multimno.core.data_objects.silver.silver_network_data_object import (
    SilverNetworkDataObject,
)
from multimno.core.data_objects.silver.silver_event_data_object import (
    SilverEventDataObject,
)
from multimno.core.data_objects.silver.silver_event_flagged_data_object import (
    SilverEventFlaggedDataObject,
)
from multimno.core.data_objects.silver.silver_semantic_quality_metrics import (
    SilverEventSemanticQualityMetrics,
)
from multimno.core.settings import CONFIG_SILVER_PATHS_KEY
from multimno.core.spark_session import (
    check_if_data_path_exists,
    delete_file_or_folder,
)
from multimno.core.log import get_execution_stats


class SemanticCleaning(Component):
    """
    Class that performs semantic checks on event data and adds error flags
    """

    COMPONENT_ID = "SemanticCleaning"

    def __init__(self, general_config_path: str, component_config_path: str) -> None:
        super().__init__(general_config_path, component_config_path)
        self.timestamp = datetime.datetime.now()

        # Read date of study
        self.date_format = self.config.get(self.COMPONENT_ID, "date_format")
        data_period_start = self.config.get(self.COMPONENT_ID, "data_period_start")
        data_period_end = self.config.get(self.COMPONENT_ID, "data_period_end")
        self.date_of_study: datetime.date = None

        self.do_different_location_deduplication = self.config.getboolean(
            self.COMPONENT_ID, "do_different_location_deduplication"
        )

        try:
            self.data_period_start = datetime.datetime.strptime(data_period_start, self.date_format).date()
        except ValueError as e:
            self.logger.error(str(e), exc_info=True)
            raise e

        try:
            self.data_period_end = datetime.datetime.strptime(data_period_end, self.date_format).date()
        except ValueError as e:
            self.logger.error(str(e), exc_info=True)
            raise e

        self.data_period_dates = [
            self.data_period_start + datetime.timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

        # Unit: metre
        self.semantic_min_distance = self.config.getfloat(self.COMPONENT_ID, "semantic_min_distance_m")

        # Unit: metre / second
        self.semantic_min_speed = self.config.getfloat(self.COMPONENT_ID, "semantic_min_speed_m_s")

    def initalize_data_objects(self):
        self.clear_destination_directory = self.config.getboolean(self.COMPONENT_ID, "clear_destination_directory")
        input_silver_event_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver")
        input_silver_network_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "network_data_silver")
        output_silver_event_path = self.config.get(CONFIG_SILVER_PATHS_KEY, "event_data_silver_flagged")
        output_silver_semantic_metrics_path = self.config.get(
            CONFIG_SILVER_PATHS_KEY, "event_device_semantic_quality_metrics"
        )

        input_silver_network = SilverNetworkDataObject(
            self.spark,
            input_silver_network_path,
        )
        input_silver_event = SilverEventDataObject(self.spark, input_silver_event_path)

        if self.clear_destination_directory:
            delete_file_or_folder(self.spark, output_silver_event_path)

        output_silver_event = SilverEventFlaggedDataObject(self.spark, output_silver_event_path)
        output_silver_semantic_metrics = SilverEventSemanticQualityMetrics(
            self.spark,
            output_silver_semantic_metrics_path,
        )

        self.input_data_objects = {
            SilverEventDataObject.ID: input_silver_event,
            SilverNetworkDataObject.ID: input_silver_network,
        }
        self.output_data_objects = {
            SilverEventFlaggedDataObject.ID: output_silver_event,
            SilverEventSemanticQualityMetrics.ID: output_silver_semantic_metrics,
        }

    def transform(self):
        events_df = self.events_df
        cells_df = self.cells_df

        # Pushup filter
        events_df = events_df.filter(
            F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(self.date_of_study)
        )

        cells_df = (
            cells_df.filter(
                F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                == F.lit(self.date_of_study)
            )
            # Point geometry auxiliar column
            .withColumn(
                "geometry",
                STC.ST_Point(F.col(ColNames.latitude), F.col(ColNames.latitude)),
            ).select(
                [
                    F.col(ColNames.cell_id),
                    F.col(ColNames.latitude).alias("cell_lat"),
                    F.col(ColNames.longitude).alias("cell_lon"),
                    F.col(ColNames.valid_date_start),
                    F.col(ColNames.valid_date_end),
                    F.col("geometry"),
                ]
            )
        )

        # Perform a left join between events and cell IDs. Non-existent cell IDs will be matched
        # with null values
        df = events_df.join(cells_df, on=ColNames.cell_id, how="left")

        # Create error flag column alongside the first error flag: non existent column
        df = self._flag_non_existent_cell_ids(df)

        # Optional flagging of duplicates with different location info
        if self.do_different_location_deduplication:
            df = self._flag_different_location_duplicates(df)

        # Error flag: Check rows which have valid date start and/or valid date end, and flag when timestamp is incompatible
        df = self._flag_invalid_cell_ids(df)

        # Error flag: suspicious and incorrect events based on location change distance and speed
        df = self._flag_by_event_location(df)

        # Keep only the necessary columns and remove auxiliar ones
        df = df.select(SilverEventFlaggedDataObject.SCHEMA.names)

        df = df.repartition(ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo)

        # df.cache()

        # Semantic metrics
        metrics_df = self._compute_semantic_metrics(df)

        self.output_data_objects[SilverEventFlaggedDataObject.ID].df = df
        self.output_data_objects[SilverEventSemanticQualityMetrics.ID].df = metrics_df

    def _flag_non_existent_cell_ids(self, df: DataFrame) -> DataFrame:
        """Method that creates a new integer column with the name of ColNames.error_flag, and
        sets the corresponding flags to events that refer to non-existent cell IDs. The rest of
        the column's values are left as null.

        Args:
            df (DataFrame): PySpark DataFrame resulting from the left join of events and cells

        Returns:
            DataFrame: same DataFrame as the input with a new error flag column, containing
                flags for the events with a non existent cell ID
        """
        df = df.withColumn(
            ColNames.error_flag,
            F.when(
                F.col("geometry").isNull(),
                F.lit(SemanticErrorType.CELL_ID_NON_EXISTENT),
            ),
        )
        return df

    def _flag_invalid_cell_ids(self, df: DataFrame) -> DataFrame:
        """Method that finds and flags events which refer to an existent cell ID, but that happened outside the
        time interval during which the cell was operationals. This flag cannot occur at the same time as a
        non-existent cell. The rest of values (i.e., those with no previous flags) in the error flag column are left
        as null.
        The auxiliar Point geometry column will be set to null for these flagged events.

        Args:
            df (DataFrame): DataFrame in which invalid cells will be flagged

        Returns:
            DataFrame: DataFrame with flagged invalid cells
        """
        df = df.withColumn(
            ColNames.error_flag,
            # Leave already flagged rows as is
            F.when(F.col(ColNames.error_flag).isNotNull(), F.col(ColNames.error_flag)).otherwise(
                # event happened before the cell was operational, or after the cell was operational
                F.when(
                    (
                        (
                            F.col(ColNames.valid_date_start).isNotNull()
                            & (F.col(ColNames.timestamp) < F.col(ColNames.valid_date_start))
                        )
                        | (
                            F.col(ColNames.valid_date_end).isNotNull()
                            & (F.col(ColNames.timestamp) > F.col(ColNames.valid_date_end))
                        )
                    ),
                    F.lit(SemanticErrorType.CELL_ID_NOT_VALID),
                )
            ),
        )

        df = df.withColumn(
            "geometry",
            F.when(F.col(ColNames.error_flag).isNotNull(), F.lit(None)).otherwise(F.col("geometry")),
        )
        return df

    def _flag_by_event_location(self, df: DataFrame) -> DataFrame:
        """Method that finds and flags events that are considered to be suspicious or incorrect in
        terms of their timestamp and cell location with respect to their previous and/or following
        events.
        It is assumed that these are the last flags to be raised. Thus, non-flagged events are also
        set to the no-error-flag value within this method.
        Args:
            df (DataFrame): DataFrame in which suspicious and/or incorrect events based on
                location are to be found and flagged

        Returns:
            DataFrame: flagged DataFrame with suspicious and/or incorrect events
        """
        # Windows that comprise all previous (following) rows ordered by time for each user.
        # Partition pruning
        # These windows have to be used, as all records have to be kept, and we skip them
        forward_window = (
            Window.partitionBy(
                [
                    ColNames.year,
                    ColNames.month,
                    ColNames.day,
                    ColNames.user_id_modulo,
                    ColNames.user_id,
                ]
            )
            .orderBy(ColNames.timestamp)
            .rowsBetween(Window.currentRow + 1, Window.unboundedFollowing)
        )
        backward_window = (
            Window.partitionBy(
                [
                    ColNames.year,
                    ColNames.month,
                    ColNames.day,
                    ColNames.user_id_modulo,
                    ColNames.user_id,
                ]
            )
            .orderBy(ColNames.timestamp)
            .rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
        )

        # Columns to be evaluated for flags
        # The order of the definition of these columns appears to affect the physical plan
        # TODO: find best ordering
        df = (
            df
            # auxiliar column containing timestamps of non-flagged events, and null for flagged events
            .withColumn(
                "filtered_ts",
                F.when(F.col(ColNames.error_flag).isNull(), F.col(ColNames.timestamp)).otherwise(None),
            )
            .withColumn(
                "next_timediff",  # time b/w curr event and first following non-flagged event timestamp
                F.when(
                    F.col(ColNames.error_flag).isNull(),
                    (
                        F.first(F.col("filtered_ts"), ignorenulls=True).over(forward_window).cast(LongType())
                        - F.col(ColNames.timestamp).cast(LongType())
                    ),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "prev_timediff",  # time b/w curr event and last previous non-flagged event timestamp
                F.when(
                    F.col(ColNames.error_flag).isNull(),
                    (
                        F.col(ColNames.timestamp).cast(LongType())
                        - F.last(F.col("filtered_ts"), ignorenulls=True).over(backward_window).cast(LongType())
                    ),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "next_distance",  # distance b/w curr location and first following non-flagged event location
                F.when(
                    F.col(ColNames.error_flag).isNull(),  # rows not flagged (yet)
                    STF.ST_DistanceSpheroid(
                        F.col("geometry"),
                        F.first(F.col("geometry"), ignorenulls=True).over(forward_window),
                    ),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "prev_distance",  # distance b/w curr location and last previous non-flagged event location
                F.when(
                    F.col(ColNames.error_flag).isNull(),  # rows not flagged (yet)
                    STF.ST_DistanceSpheroid(
                        F.col("geometry"),
                        F.last(F.col("geometry"), ignorenulls=True).over(backward_window),
                    ),
                ).otherwise(F.lit(None)),
            )
            .withColumn(  # mean speed in m/s for euclidean displacemente b/w curr and next non-flagged events
                "next_speed", F.col("next_distance") / F.col("next_timediff")
            )
            .withColumn(  # mean speed in m/s for euclidean displacemente b/w curr and previous non-flagged events
                "prev_speed", F.col("prev_distance") / F.col("prev_timediff")
            )
        )

        # Conditions that must occur for the two location related error flags
        incorrect_location_cond = (
            (F.col("prev_speed") > self.semantic_min_speed)
            & (F.col("prev_distance") > self.semantic_min_distance)
            & (F.col("next_speed") > self.semantic_min_speed)
            & (F.col("next_distance") > self.semantic_min_distance)
        )

        suspicious_location_cond = F.coalesce(
            (F.col("prev_speed") > self.semantic_min_speed) & (F.col("prev_distance") > self.semantic_min_distance),
            F.lit(False),
        ) | F.coalesce(
            (F.col("next_speed") > self.semantic_min_speed) & (F.col("next_distance") > self.semantic_min_distance),
            F.lit(False),
        )
        # Set the error flags
        # NOTE: it is assumed that this is the last flag to be computed. Thus, all non-flagged events
        # will be set to the code corresponding to no error flags. If new flags are to be added, one might
        # want to change this.
        df = df.withColumn(
            ColNames.error_flag,
            F.when(
                F.col(ColNames.error_flag).isNull(),  # for non-flagged events
                F.when(
                    incorrect_location_cond,
                    F.lit(SemanticErrorType.INCORRECT_EVENT_LOCATION),
                ).otherwise(
                    F.when(
                        suspicious_location_cond,
                        F.lit(SemanticErrorType.SUSPICIOUS_EVENT_LOCATION),
                    ).otherwise(F.lit(SemanticErrorType.NO_ERROR))
                ),
            ).otherwise(F.col(ColNames.error_flag)),
        )

        return df

    def _flag_different_location_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Method that checkes for duplicates of a different location type and flags these rows with the corresponding error flag.
        A different location duplicate is such where user_id and timestamp columns are identical,
        but any of the cell_id, latitude or longitude columns are different.
        In the current implementation, all column rows are counted for a given partition of user_id_modulo, user_id and timestamp.

        Args:
            df (DataFrame): PySpark DataFrame resulting from the left join of events and cells

        Returns:
            DataFrame: same DataFrame as the input with a new error flag column, containing
                flags for the events with identical timestamps, but different cell_id or latitude or longitude column values
        """

        # Hash the columns that define a unique location
        df = df.withColumn(
            "location_hash", F.hash(ColNames.cell_id, ColNames.latitude, ColNames.longitude, ColNames.plmn)
        )

        # Define a window partitioned by user_id only
        window = Window.partitionBy(
            ColNames.year, ColNames.month, ColNames.day, ColNames.user_id_modulo, ColNames.user_id
        ).orderBy(ColNames.timestamp)

        # Mark rows with the same user_id and timestamp but different location hashes as duplicates
        df = df.withColumn(
            ColNames.error_flag,
            F.when(
                (
                    (F.lag("location_hash", 1).over(window) != F.col("location_hash"))
                    & (F.lag(ColNames.timestamp, 1).over(window) == F.col(ColNames.timestamp))
                )
                | (
                    (F.lead("location_hash", 1).over(window) != F.col("location_hash"))
                    & (F.lead(ColNames.timestamp, 1).over(window) == F.col(ColNames.timestamp))
                ),
                F.lit(SemanticErrorType.DIFFERENT_LOCATION_DUPLICATE),
            ).otherwise(F.col(ColNames.error_flag)),
        )

        return df

    def _compute_semantic_metrics(self, df: DataFrame) -> DataFrame:
        """Method that computes the semantic quality metrics of the semantic checks.
        This amounts to counting the number of flagged events after the semantic checks.

        Args:
            df (DataFrame): Flagged event DataFrame

        Returns:
            DataFrame: semantic metrics DataFrame
        """
        metrics_df = (
            df.groupby(ColNames.error_flag)
            .agg(F.count(F.col(ColNames.error_flag)).alias(ColNames.value))
            .withColumnRenamed(ColNames.error_flag, ColNames.type_of_error)
            .withColumns(
                {
                    ColNames.variable: F.lit(ColNames.cell_id),  # currently, only cell_id here
                    ColNames.year: F.lit(self.date_of_study.year).cast(ShortType()),
                    ColNames.month: F.lit(self.date_of_study.month).cast(ByteType()),
                    ColNames.day: F.lit(self.date_of_study.day).cast(ByteType()),
                }
            )
        )

        all_error_codes = [error_name for error_name in dir(SemanticErrorType) if not error_name.startswith("__")]
        all_error_codes = [
            Row(
                **{
                    ColNames.type_of_error: getattr(SemanticErrorType, error_name),
                }
            )
            for error_name in all_error_codes
        ]

        all_errors_df = self.spark.createDataFrame(
            all_error_codes,
            schema=StructType([SilverEventSemanticQualityMetrics.SCHEMA[ColNames.type_of_error]]),
        )

        metrics_df = (
            metrics_df.join(all_errors_df, on=ColNames.type_of_error, how="right")
            .fillna(
                {
                    ColNames.variable: ColNames.cell_id,
                    ColNames.value: 0,
                    ColNames.year: self.date_of_study.year,
                    ColNames.month: self.date_of_study.month,
                    ColNames.day: self.date_of_study.day,
                }
            )
            .withColumn(ColNames.result_timestamp, F.lit(self.timestamp))
        )

        return metrics_df

    @get_execution_stats
    def execute(self):
        """
        Method that performs the read, transform and write methods of the component.
        """
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        for date in self.data_period_dates:
            self.date_of_study = date

            self.events_df = self.input_data_objects[SilverEventDataObject.ID].df.filter(
                F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                == F.lit(self.date_of_study)
            )
            self.cells_df = self.input_data_objects[SilverNetworkDataObject.ID].df.filter(
                F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day))
                == F.lit(self.date_of_study)
            )

            self.logger.info(f"Processing data for {date}")
            self.transform()
            self.write()
            self.logger.info(f"Finished {date}")

        self.logger.info(f"Finished {self.COMPONENT_ID}")
