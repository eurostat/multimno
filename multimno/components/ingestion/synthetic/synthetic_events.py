"""
This module contains the SyntheticEvents class, which is responsible for generating the synthetic event data.
"""

import random
import string
from datetime import datetime, timedelta

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType, BinaryType, StringType

from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP

from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_event_data_object import (
    BronzeEventDataObject,
)
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import (
    BronzeNetworkDataObject,
)
from multimno.core.data_objects.bronze.bronze_synthetic_diaries_data_object import (
    BronzeSyntheticDiariesDataObject,
)
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.log import get_execution_stats

class SyntheticEvents(Component):
    """
    Class that generates the event synthetic data. It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticEvents"

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(
            general_config_path=general_config_path,
            component_config_path=component_config_path,
        )

        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        self.event_freq_stays = self.config.getint(self.COMPONENT_ID, "event_freq_stays")
        self.event_freq_moves = self.config.getint(self.COMPONENT_ID, "event_freq_moves")
        self.closest_cell_distance_max = self.config.getint(self.COMPONENT_ID, "closest_cell_distance_max")
        self.closest_cell_distance_max_for_errors = self.config.getint(
            self.COMPONENT_ID, "closest_cell_distance_max_for_errors"
        )
        self.error_location_probability = self.config.getfloat(self.COMPONENT_ID, "error_location_probability")
        self.error_location_distance_min = self.config.getint(self.COMPONENT_ID, "error_location_distance_min")
        self.error_location_distance_max = self.config.getint(self.COMPONENT_ID, "error_location_distance_max")
        self.cartesian_crs = self.config.getint(self.COMPONENT_ID, "cartesian_crs")
        self.error_cell_id_probability = self.config.getfloat(self.COMPONENT_ID, "error_cell_id_probability")
        self.maximum_number_of_cells_for_event = self.config.getfloat(
            self.COMPONENT_ID, "maximum_number_of_cells_for_event"
        )

        self.mcc = self.config.getint(self.COMPONENT_ID, "mcc")
        self.mnc = self.config.get(self.COMPONENT_ID, "mnc")

        # Parameters for synthetic event errors generation (these are not locational errors)

        self.do_event_error_generation = self.config.getboolean(self.COMPONENT_ID, "do_event_error_generation")
        self.column_is_null_probability = self.config.getfloat(self.COMPONENT_ID, "column_is_null_probability")
        self.null_row_prob = self.config.getfloat(self.COMPONENT_ID, "null_row_probability")
        self.data_type_error_prob = self.config.getfloat(self.COMPONENT_ID, "data_type_error_probability")
        self.out_of_bounds_prob = self.config.getfloat(self.COMPONENT_ID, "out_of_bounds_probability")
        self.same_location_duplicate_prob = self.config.getfloat(
            self.COMPONENT_ID, "same_location_duplicates_probability"
        )
        self.different_location_duplicate_prob = self.config.getfloat(
            self.COMPONENT_ID, "different_location_duplicates_probability"
        )

        self.mandatory_columns = [i.name for i in BronzeEventDataObject.SCHEMA]
        self.error_generation_allowed_columns = set(self.mandatory_columns) - set(
            [ColNames.year, ColNames.month, ColNames.day]
        )

        self.order_output_by_timestamp = self.config.getboolean(self.COMPONENT_ID, "order_output_by_timestamp")
        
        self.data_period_start = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_start"), "%Y-%m-%d"
        ).date()
        self.data_period_end = datetime.strptime(
            self.config.get(self.COMPONENT_ID, "data_period_end"), "%Y-%m-%d"
        ).date()

        self.data_period_dates = [
            self.data_period_start + timedelta(days=i)
            for i in range((self.data_period_end - self.data_period_start).days + 1)
        ]

    def initalize_data_objects(self):

        pop_diares_input_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "diaries_data_bronze")
        pop_diaries_bronze_event = BronzeSyntheticDiariesDataObject(
            self.spark,
            pop_diares_input_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        # Input for cell attributes
        network_data_input_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "network_data_bronze")

        cell_locations_bronze = BronzeNetworkDataObject(
            self.spark,
            network_data_input_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        self.input_data_objects = {
            BronzeSyntheticDiariesDataObject.ID: pop_diaries_bronze_event,
            BronzeNetworkDataObject.ID: cell_locations_bronze,
        }

        output_records_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        bronze_event = BronzeEventDataObject(
            self.spark,
            output_records_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )

        self.output_data_objects = {BronzeEventDataObject.ID: bronze_event}

    @get_execution_stats
    def execute(self):
        self.logger.info(f"Starting {self.COMPONENT_ID}...")
        self.read()

        for current_date in self.data_period_dates:

            self.logger.info(f"Processing diaries for {current_date.strftime('%Y-%m-%d')}")

            self.current_diaries = self.input_data_objects[BronzeSyntheticDiariesDataObject.ID].df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            )

            self.current_cells = self.input_data_objects[BronzeNetworkDataObject.ID].df.filter(
                (F.make_date(F.col(ColNames.year), F.col(ColNames.month), F.col(ColNames.day)) == F.lit(current_date))
            ).select(
                ColNames.cell_id,
                ColNames.latitude,
                ColNames.longitude,
                ColNames.year,
                ColNames.month,
                ColNames.day,
            )

            self.transform()
            self.write()

        self.logger.info(f"Finished {self.COMPONENT_ID}")


    def transform(self):

        pop_diaries_df = self.current_diaries
        cells_df = self.current_cells

        # Filtering stays to get the lat and lon of movement starting and end point
        stays_df = pop_diaries_df.filter(F.col(ColNames.activity_type) == "stay")

        move_events_df = self.generate_event_timestamps_for_moves(
            stays_df, self.event_freq_moves, self.cartesian_crs, self.seed
        )
        move_events_with_locations_df = self.generate_locations_for_moves(move_events_df, self.cartesian_crs)

        stay_events_df = self.generate_event_timestamps_for_stays(
            stays_df, self.event_freq_stays, self.cartesian_crs, self.seed
        )

        generated_stays_and_moves = stay_events_df.union(move_events_with_locations_df)

        # Add geometry column to cells
        cells_df = cells_df.withColumn(
            "cell_geometry",
            STF.ST_Transform(
                STC.ST_Point(F.col(ColNames.longitude), F.col(ColNames.latitude)),
                F.lit("EPSG:4326"),
                F.lit(f"EPSG:{self.cartesian_crs}"),
            ),
        ).select(ColNames.cell_id, "cell_geometry")

        # 1) From the clean records, sample records for location errors
        sampled_records = generated_stays_and_moves.sample(self.error_location_probability, self.seed)
        generated_stays_and_moves = generated_stays_and_moves.subtract(sampled_records)
        generated_stays_and_moves = generated_stays_and_moves.withColumn(
            ColNames.loc_error, F.lit(None).cast(FloatType())
        )

        records_with_location_errors = self.generate_location_errors(
            sampled_records,
            self.error_location_distance_max,
            self.error_location_distance_min,
            self.closest_cell_distance_max_for_errors,
            self.cartesian_crs,
            self.seed,
        )

        # 2) From the clean records, sample records for erroneous cell id creation
        sampled_records = generated_stays_and_moves.sample(self.error_cell_id_probability, self.seed)
        records_with_cell_id_errors = self.generate_records_with_non_existant_cell_ids(
            sampled_records, cells_df, self.seed
        )

        generated_stays_and_moves = generated_stays_and_moves.subtract(sampled_records)
        generated_stays_and_moves = generated_stays_and_moves.withColumn(
            "closest_cell_distance_max", F.lit(self.closest_cell_distance_max)
        )

        # Label error rows so that these would be ignored in syntactic error generation
        records_with_location_errors = records_with_location_errors.withColumn("is_modified", F.lit(True))
        records_with_cell_id_errors = records_with_cell_id_errors.withColumn("is_modified", F.lit(True))
        generated_stays_and_moves = generated_stays_and_moves.withColumn("is_modified", F.lit(False))

        # 3) Link a cell id to each location

        records_sdf = generated_stays_and_moves.union(records_with_location_errors)

        records_sdf = self.add_cell_ids_to_locations(
            records_sdf,
            cells_df,
            self.maximum_number_of_cells_for_event,
            self.seed,
        )

        # 4) Continuing with the combined dataframe

        records_sdf = records_sdf.union(records_with_cell_id_errors)

        records_sdf = records_sdf.dropDuplicates([ColNames.user_id, ColNames.timestamp])

        records_sdf = records_sdf.withColumn(
            "generated_geometry",
            STF.ST_Transform(
                "generated_geometry",
                F.lit(f"EPSG:{self.cartesian_crs}"),
                F.lit("EPSG:4326"),
            ),
        )

        records_sdf = (
            records_sdf.withColumn(ColNames.longitude, STF.ST_X(F.col("generated_geometry")))
            .withColumn(ColNames.latitude, STF.ST_Y(F.col("generated_geometry")))
            .drop("generated_geometry")
        )

        # TODO: add rows with PLMN
        # MCC and loc_error are added to the records
        records_sdf = records_sdf.withColumn(ColNames.mcc, F.lit(self.mcc).cast(IntegerType()))

        records_sdf = records_sdf.withColumn(ColNames.mnc, F.lit(self.mnc))

        records_sdf = records_sdf.withColumn(ColNames.plmn, F.lit(None).cast(IntegerType()))

        records_sdf = records_sdf.withColumn(ColNames.year, F.year(F.col(ColNames.timestamp)))
        records_sdf = records_sdf.withColumn(ColNames.month, F.month(F.col(ColNames.timestamp)))
        records_sdf = records_sdf.withColumn(ColNames.day, F.dayofmonth(F.col(ColNames.timestamp)))

        # Generate errors

        if self.do_event_error_generation:
            records_sdf = self.generate_errors(synth_df_raw=records_sdf)

        # Select bronze schema columns
        columns = {field.name: F.col(field.name).cast(field.dataType) for field in BronzeEventDataObject.SCHEMA.fields}
        records_sdf = records_sdf.withColumns(columns)

        if self.order_output_by_timestamp:
            records_sdf = records_sdf.orderBy(ColNames.timestamp)

        self.output_data_objects[BronzeEventDataObject.ID].df = records_sdf

    @staticmethod
    def generate_event_timestamps_for_moves(
        stays_sdf: DataFrame, event_freq_moves: int, cartesian_crs: int, seed: int
    ) -> DataFrame:
        """
        Generates a DataFrame of event timestamps for movements between stays.

        For each stay in the input DataFrame, this method generates a random number of timestamps equal to the time
            difference between the end of the current stay and the start of the next stay, divided by the event
            frequency for moves.

        Args:
            stays_sdf (pyspark.sql.DataFrame): A DataFrame of stays.
            event_freq_moves (int): The frequency of events for movements.
            cartesian_crs (int): The EPSG code of the Cartesian coordinate reference system to use for the geometries.
            seed (int): The seed that determines the randomness of timestamp generation, and subsequent lat/lon generation.

        Returns:
            pyspark.sql.DataFrame: A DataFrame of event timestamps for movements.
        """

        # Since the rows with activity_type = movement don't have any locations in the population diaries,
        # we select the stay points and start generating timestamps in between the start and end of the stay

        stays_sdf = stays_sdf.withColumn(
            ColNames.geometry,
            STF.ST_Transform(
                STC.ST_Point(F.col(ColNames.longitude), F.col(ColNames.latitude)),
                F.lit("EPSG:4326"),
                F.lit(f"EPSG:{cartesian_crs}"),
            ),
        )

        # Define the window specification •
        window_spec = Window.partitionBy(ColNames.year, ColNames.month, ColNames.day, ColNames.user_id).orderBy(
            ColNames.initial_timestamp
        )

        # Add columns for next stay's geometry and start timestamp using the lead function
        stays_sdf = stays_sdf.withColumn(
            "next_stay_initial_timestamp",
            F.lead(ColNames.initial_timestamp, 1).over(window_spec),
        )

        stays_sdf = stays_sdf.withColumn("next_stay_geometry", F.lead(ColNames.geometry, 1).over(window_spec))

        stays_sdf = stays_sdf.withColumn(
            "time_diff_seconds",
            F.unix_timestamp("next_stay_initial_timestamp") - F.unix_timestamp(ColNames.final_timestamp),
        )

        # Calculate how many timestamps fit in the interval for the given frequency
        stays_sdf = stays_sdf.withColumn(
            "timestamps_count",
            (F.col("time_diff_seconds") / event_freq_moves).cast("integer"),
        )

        expr_str = f"transform(sequence(1, timestamps_count), x -> rand({seed}))"

        stays_sdf = stays_sdf.withColumn(
            "random_fraction_on_line",
            F.expr(expr_str),
        ).withColumn("random_fraction_on_line", F.explode(F.col("random_fraction_on_line")))

        # Generate timestamps
        stays_sdf = stays_sdf.withColumn(
            "offset_seconds",
            F.col("random_fraction_on_line") * F.col("time_diff_seconds"),
        )
        stays_sdf = stays_sdf.withColumn(
            ColNames.timestamp,
            F.from_unixtime(F.unix_timestamp(ColNames.final_timestamp) + F.col("offset_seconds")),
        )

        moves_sdf = stays_sdf.withColumn(ColNames.activity_type, F.lit("move"))

        # Keep only necessary columns
        moves_sdf = moves_sdf.select(
            ColNames.user_id,
            ColNames.timestamp,
            ColNames.geometry,
            "next_stay_geometry",
            "random_fraction_on_line",
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        return moves_sdf

    @staticmethod
    def generate_event_timestamps_for_stays(
        stays_df: DataFrame, event_freq_stays: int, cartesian_crs: int, seed: int
    ) -> DataFrame:
        """
        Generates a DataFrame of event timestamps for stays based on the event frequency for stays.

        For each stay in the input DataFrame, this method calculates
        the time difference between the initial and final timestamps of the stay.
        It then generates a number of timestamps equal to this time difference divided by the event
        frequency for stays. Each timestamp is associated with the location of the stay.

        Args:
            stays_df (pyspark.sql.DataFrame): A DataFrame of stays.
            event_freq_stays (int): The frequency of events for stays.
            cartesian_crs (int): The EPSG code of the Cartesian coordinate reference system to use for the geometries.
            seed (int): The seed for generating timestamps randomly.
        Returns:
            pyspark.sql.DataFrame: A DataFrame of event timestamps for stays.
        """

        stays_df = stays_df.withColumn(
            "time_diff_seconds",
            F.unix_timestamp(F.col(ColNames.final_timestamp)) - F.unix_timestamp(F.col(ColNames.initial_timestamp)),
        )

        stays_df = stays_df.withColumn(
            "timestamps_count",
            (F.col("time_diff_seconds") / event_freq_stays).cast("integer"),
        )

        expr_str = f"transform(sequence(1, timestamps_count), x -> rand({seed}))"
        stays_df = stays_df.withColumn(
            "random_fraction_between_timestamps",
            F.expr(expr_str),
        )

        stays_df = stays_df.withColumn(
            "random_fraction_between_timestamps",
            F.explode(F.col("random_fraction_between_timestamps")),
        )
        stays_df = stays_df.withColumn(
            "offset_seconds",
            F.col("random_fraction_between_timestamps") * F.col("time_diff_seconds"),
        )
        stays_df = stays_df.withColumn(
            ColNames.timestamp,
            F.from_unixtime(F.unix_timestamp(ColNames.initial_timestamp) + F.col("offset_seconds")),
        )

        stays_df = stays_df.withColumn(
            "generated_geometry",
            STF.ST_Transform(
                STC.ST_Point(F.col(ColNames.longitude), F.col(ColNames.latitude)),
                F.lit("EPSG:4326"),
                F.lit(f"EPSG:{cartesian_crs}"),
            ),
        )

        stays_df = stays_df.select(
            ColNames.user_id,
            ColNames.timestamp,
            "generated_geometry",
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        return stays_df

    @staticmethod
    def generate_locations_for_moves(event_timestamps_df: DataFrame, cartesian_crs: int) -> DataFrame:
        """
        Generates locations for moves based on the event timestamps dataframe.
        Returns a dataframe, where for each move in the event timestamps dataframe
        a geometry column is added, representing the location of the move.

        Performs interpolation along the line between the starting move point (previous stay point)
        and next move point (next stay point) using the randomly generated values in the column random_fraction_on_line.

        Args:
            event_timestamps_df (pyspark.sql.DataFrame): The event timestamps dataframe.

        Returns:
            pyspark.sql.DataFrame: The event timestamps dataframe with the added geometry column.
        """

        moves_with_geometry = event_timestamps_df.withColumn(
            "line",
            STF.ST_MakeLine(F.col(ColNames.geometry), F.col("next_stay_geometry")),
        )
        moves_with_geometry = moves_with_geometry.withColumn(
            "generated_geometry",
            STF.ST_SetSRID(
                STF.ST_LineInterpolatePoint(F.col("line"), F.col("random_fraction_on_line")),
                cartesian_crs,
            ),
        )

        moves_with_geometry = moves_with_geometry.select(
            ColNames.user_id,
            ColNames.timestamp,
            "generated_geometry",
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        return moves_with_geometry

    @staticmethod
    def add_cell_ids_to_locations(
        events_with_locations_df: DataFrame,
        cells_df: DataFrame,
        max_n_of_cells: int,
        seed: int,
    ) -> DataFrame:
        """
        Links cell IDs to locations in the events DataFrame.

        This method performs a spatial join between the events and cells DataFrames to add cell IDs to each event.
        It first creates a buffer around each event location and finds cells that intersect with this buffer.
        It then calculates the distance from each event location to the cell and ranks the cells based on this distance.
        It keeps only the top 'max_n_of_cells' closest cells for each event.

        The method also adds a random index to each event-cell pair and filters to keep only one pair per event,
        randomly selecting one of the closest cells for each event.

        Args:
            events_with_locations_df (pyspark.sql.DataFrame): A DataFrame of events
            cells_df (pyspark.sql.DataFrame): A DataFrame of cells
            max_n_of_cells (int): The maximum number of closest cells to consider for each event.

        Returns:
            pyspark.sql.DataFrame: A DataFrame of events with cell IDs added.
        """
        events_with_cells_sdf = events_with_locations_df.join(
            cells_df[[ColNames.cell_id, "cell_geometry"]],
            (
                STP.ST_Intersects(
                    STF.ST_Buffer(
                        events_with_locations_df["generated_geometry"],
                        F.col("closest_cell_distance_max"),
                    ),
                    cells_df["cell_geometry"],
                )
            ),
        ).withColumn(
            "distance_to_cell",
            STF.ST_Distance(
                events_with_locations_df["generated_geometry"],
                cells_df["cell_geometry"],
            ),
        )

        # Selection of different cell ids for a given timestamp is random
        window_spec = Window.partitionBy(
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id,
            ColNames.timestamp,
        ).orderBy(F.col("distance_to_cell"))

        events_with_cells_sdf = events_with_cells_sdf.withColumn(
            "closest_cells_index", F.row_number().over(window_spec)
        ).filter(F.col("closest_cells_index") <= max_n_of_cells)

        window_spec_random = Window.partitionBy(
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.user_id,
            ColNames.timestamp,
        ).orderBy(F.rand(seed=seed))
        events_with_cells_sdf = events_with_cells_sdf.withColumn(
            "random_cell_index", F.row_number().over(window_spec_random)
        ).filter(F.col("random_cell_index") == 1)

        records_sdf = events_with_cells_sdf.select(
            ColNames.user_id,
            ColNames.timestamp,
            "generated_geometry",
            ColNames.cell_id,
            ColNames.loc_error,
            ColNames.year,
            ColNames.month,
            ColNames.day,
            "is_modified",
        )

        return records_sdf

    @staticmethod
    def generate_location_errors(
        records_sdf: DataFrame,
        error_location_distance_max: float,
        error_location_distance_min: float,
        closest_cell_distance_max: float,
        cartesian_crs: int,
        seed: int,
    ) -> DataFrame:
        """
        Generates location errors for x and y coordinates of each record in the DataFrame.

        This method adds a random location error to the x and y coordinates of each record in the input DataFrame.
        The location error is a random value between error_location_distance_min and error_location_distance_max,
        and is added or subtracted from the x and y coordinates based on a random sign.

        Args:
            records_sdf (DataFrame): A DataFrame of records
            error_location_distance_max (float): The maximum location error distance.
            error_location_distance_min (float): The minimum location error distance.
            closest_cell_distance_max (float): The maximum distance to the closest cell.
            cartesian_crs (int): The EPSG code of the Cartesian coordinate reference system to use for the geometries.
            seed (int): The seed for the random number generator.

        Returns:
            DataFrame: A DataFrame of records with location errors added to the x and y coordinates.
        """

        errors_df = (
            records_sdf.withColumn("y", STF.ST_Y(F.col("generated_geometry")))
            .withColumn("x", STF.ST_X(F.col("generated_geometry")))
            .drop("generated_geometry")
        )

        errors_df = errors_df.withColumn(
            ColNames.loc_error,
            (F.rand(seed=seed) * (error_location_distance_max - error_location_distance_min))
            + error_location_distance_min,
        )

        errors_df = (
            errors_df.withColumn("sign", F.when(F.rand(seed=seed) > 0.5, 1).otherwise(-1))
            .withColumn("new_x", F.col("x") + (F.col(ColNames.loc_error) * F.col("sign")))
            .withColumn("new_y", F.col("y") + (F.col(ColNames.loc_error) * F.col("sign")))
        )

        errors_df = errors_df.withColumn(
            "generated_geometry",
            STF.ST_SetSRID(STC.ST_Point(F.col("new_x"), F.col("new_y")), cartesian_crs),
        ).drop("new_x", "new_y")

        errors_df = errors_df.withColumn("closest_cell_distance_max", F.lit(closest_cell_distance_max))

        errors_df = errors_df.select(
            ColNames.user_id,
            ColNames.timestamp,
            "generated_geometry",
            ColNames.year,
            ColNames.month,
            ColNames.day,
            ColNames.loc_error,
            "closest_cell_distance_max",
        )

        return errors_df

    @staticmethod
    def generate_records_with_non_existant_cell_ids(records_sdf: DataFrame, cells_sdf: DataFrame, seed) -> DataFrame:
        """
        Adds the cell_id column so that it will contain cell_ids that
        are not present in the cells_df dataframe, yet follow the format of a cell id.

        Args:
            records_sdf (DataFrame): generated records
            cells_sdf (DataFrame): cells dataframe

            DataFrame: records with cell ids that are not present in the cells_df dataframe
        """

        # Generates random cell ids for cells_df, and selects those
        # Join to records is implemented with a monotonically increasing id
        # So to limit that, this number of all unique cells is used
        # TODO check how to make this more optimal

        n_of_actual_cell_ids_as_base = cells_sdf.count()

        cells_df_with_random_cell_ids = cells_sdf[[ColNames.cell_id]].withColumn(
            "random_cell_id",
            (F.rand(seed=seed) * (999_999_999_999_999 - 10_000_000_000_000) + 10_000_000_000_000).cast("bigint"),
        )

        # Do an  left anti join to ensure that now generated cell ids are not among actual cell ids

        cells_df_inner_joined = cells_df_with_random_cell_ids[["random_cell_id"]].join(
            cells_sdf[[ColNames.cell_id]],
            on=F.col("random_cell_id") == F.col(ColNames.cell_id),
            how="leftanti",
        )

        cells_df_inner_joined = cells_df_inner_joined.withColumn("row_number", (F.monotonically_increasing_id()))
        records_sdf = records_sdf.withColumn(
            "row_number",
            (F.monotonically_increasing_id() % n_of_actual_cell_ids_as_base) + 1,
        )

        cells_df_inner_joined = cells_df_inner_joined.select(
            "row_number", F.col("random_cell_id").alias(ColNames.cell_id)
        )

        records_with_random_cell_id = records_sdf.join(cells_df_inner_joined, on="row_number", how="left").drop(
            "row_number"
        )

        records_with_random_cell_id = records_with_random_cell_id.select(
            ColNames.user_id,
            ColNames.timestamp,
            "generated_geometry",
            ColNames.cell_id,
            ColNames.loc_error,
            ColNames.year,
            ColNames.month,
            ColNames.day,
        )

        return records_with_random_cell_id

    # @staticmethod
    def generate_nulls_in_mandatory_fields(self, df: DataFrame) -> DataFrame:
        """
        Generates null values in some fields of some rows based on configuration parameters.

        Args:
            df (pyspark.sql.DataFrame): clean synthetic data

        Returns:
            pyspark.sql.DataFrame: synthetic records dataframe with nulls in some columns of some rows
        """

        # Two probability parameters from config apply:
        # First one sets how many rows (as fraction of all rows) are selected for possible value nulling.
        # Second one sets the likelyhood for each column to be set to null.
        # If 1.0, then all mandatory columns of each selected row will be nulled.

        if self.null_row_prob == 0.0:
            # TODO logging
            return df

        # Split input dataframe to unchanged and changed portions
        df = df.cache()
        error_row_prob = self.null_row_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = df.randomSplit([unchanged_row_prob, error_row_prob], seed=self.seed + 1)

        columns_for_null_selection = list(self.error_generation_allowed_columns)
        columns_for_null_selection.sort()

        random.seed(self.seed)
        columns_to_set_as_null = random.sample(
            columns_for_null_selection,
            int(round(self.column_is_null_probability * len(columns_for_null_selection), 0)),
        )

        for column in columns_to_set_as_null:
            error_rows_df = error_rows_df.withColumn(column, F.lit(None))

        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))

        # Re-combine unchanged and changed rows of the dataframe.
        return unchanged_rows_df.union(error_rows_df)

    # @staticmethod
    def generate_out_of_bounds_dates(self, df: DataFrame) -> DataFrame:
        """
        Transforms the timestamp column values to be out of bound of the selected period,
        based on probabilities from configuration.
        Only rows with non-null timestamp values can become altered here.

        Args:
            df (pyspark.sql.DataFrame): Dataframe of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Data where some timestamp column values are out of bounds as per config.
        """

        if self.out_of_bounds_prob == 0.0:
            # TODO logging
            return df

        # Calculate approximate span in months from config parameters.
        # The parameters should approximately cover the range of "clean" dates, so that the erroneous values can be generated outside the range.

        # TODO
        # This now uses the whole input data to set the bounds
        ending_timestamp = df.agg(F.max(ColNames.timestamp)).collect()[0][0]
        starting_timestamp = df.agg(F.min(ColNames.timestamp)).collect()[0][0]
        events_span_in_months = max(1, (pd.Timestamp(ending_timestamp) - pd.Timestamp(starting_timestamp)).days / 30)

        # Split rows by null/non-null timestamp.
        df = df.cache()
        null_timestamp_df = df.where(F.col(ColNames.timestamp).isNull())
        nonnull_timestamp_df = df.where(F.col(ColNames.timestamp).isNotNull())
        df.unpersist()

        # From non-null timestamp rows, select a subset for adding errors to, depending on config parameter.
        nonnull_timestamp_df = nonnull_timestamp_df.cache()
        error_row_prob = self.out_of_bounds_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = nonnull_timestamp_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed + 3
        )
        # Combine null timestamp rows and not-modified non-null timestamp rows.
        unchanged_rows_df = unchanged_rows_df.union(null_timestamp_df)

        # Add months offset to error rows to make their timestamp values become outside expected range.
        months_to_add_col = (F.lit(2) + F.rand(seed=self.seed)) * F.lit(events_span_in_months)
        modified_date_col = F.add_months(F.col(ColNames.timestamp), months_to_add_col)
        time_col = F.date_format(F.col(ColNames.timestamp), " HH:mm:ss")
        error_rows_df = error_rows_df.withColumn(ColNames.timestamp, F.concat(modified_date_col, time_col))
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))

        # Combine changed and unchanged rows dataframes.
        return unchanged_rows_df.union(error_rows_df)

    # @staticmethod
    def generate_erroneous_type_values(self, df: DataFrame) -> DataFrame:
        """
        Generates errors for sampled rows. Errors are custom defined, for instance a random string, or corrupt timestamp.
        Does not cast the columns to a different type.

        Args:
            df (pyspark.sql.DataFrame): dataframe that may have out of bound and null records.

        Returns:
            pyspark.sql.DataFrame: dataframe with erroneous rows, and possibly, with nulls and out of bound records.
        """

        if self.data_type_error_prob == 0:
            # TODO logging
            return df

        # Split dataframe by whether the row has been modified during the error-adding process already.
        # Already errored rows do not get further changes.
        df = df.cache()
        previously_modified_rows_df = df.where(F.col("is_modified"))
        unmodified_rows_df = df.where(~(F.col("is_modified")))
        df.unpersist()

        # From unmodified rows, select a subset for adding errors to, depending on config parameter.
        unmodified_rows_df = unmodified_rows_df.cache()
        error_row_prob = self.data_type_error_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = unmodified_rows_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed + 5
        )
        # Gather rows that are not modified in this step (combine previously-modified rows and not-selected unmodified rows).
        unchanged_rows_df = unchanged_rows_df.union(previously_modified_rows_df)

        # Iterate over mandatory columns to mutate the value, depending on column data type.
        for struct_schema in BronzeEventDataObject.SCHEMA:
            if struct_schema.name not in self.error_generation_allowed_columns:
                continue

            column = struct_schema.name
            col_dtype = struct_schema.dataType

            if col_dtype in [BinaryType()]:
                # md5 is a smaller hash,
                to_value = F.unhex(F.md5(F.base64(F.col(column)).cast(StringType())))

            if col_dtype in [FloatType(), IntegerType()]:
                # changes mcc, lat, lon
                to_value = (F.col(column) + ((F.rand(seed=self.seed) + F.lit(180)) * 10000)).cast("int")

            if column == ColNames.timestamp and col_dtype == StringType():
                # Timezone difference manipulation may be performed here, if cleaning module were to support it.
                # statically one timezone difference
                # timezone_to = random.randint(0, 12)
                to_value = F.concat(
                    F.substring(F.col(column), 1, 10),
                    F.lit("T"),
                    F.substring(F.col(column), 12, 9),
                    # TODO: Temporary remove of timezone addition as cleaning
                    # module does not support it
                    # F.lit(f"+0{timezone_to}:00")
                )

            if column == ColNames.cell_id and col_dtype == StringType():
                random.seed(self.seed)
                random_string = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6)) + "_"
                to_value = F.concat(F.lit(random_string), (F.rand(seed=self.seed) * 100).cast("int"))

            error_rows_df = error_rows_df.withColumn(column, to_value)
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))
        return unchanged_rows_df.union(error_rows_df)

    def generate_same_location_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Selects a subset of the previously generated data (syntactically clean data) and creates same location duplicates.
        If input has odd number of rows, one row is discarded.

        Args:
            df (pyspark.sql.DataFrame): Dataframe of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Dataframe with same location duplicates.

        """

        if self.same_location_duplicate_prob == 0:
            return df

        # Split dataframe by whether the row has been modified during the error-adding process already.
        # Already errored rows do not get further changes.
        df = df.cache()
        previously_modified_rows_df = df.where(F.col("is_modified"))
        unmodified_rows_df = df.where(~(F.col("is_modified")))
        df.unpersist()

        # From unmodified rows, select a subset for adding errors to, depending on config parameter.
        unmodified_rows_df = unmodified_rows_df.cache()
        error_row_prob = self.same_location_duplicate_prob
        unchanged_row_prob = 1.0 - error_row_prob

        unchanged_rows_df, error_rows_df = unmodified_rows_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed + 7
        )

        # Gather rows that are not modified in this step (combine previously-modified rows and not-selected unmodified rows).
        unchanged_rows_df = unchanged_rows_df.union(previously_modified_rows_df)

        # Select all even rows and duplicate these
        error_rows_df = error_rows_df.withColumn(
            "user_row_num",
            F.row_number().over(Window.partitionBy(ColNames.user_id).orderBy(ColNames.timestamp)),
        )

        even_rows = error_rows_df.where(F.col("user_row_num") % 2 == 0).drop("user_row_num")

        error_rows_df = even_rows.union(even_rows)
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))

        return unchanged_rows_df.union(error_rows_df)

    def generate_different_location_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Selects a subset of the previously generated data (syntactically clean data) and creates different location duplicates.
        If input has odd number of rows, one row is discarded.

        Args:
            df (pyspark.sql.DataFrame): Dataframe of clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Dataframe with same different location duplicates.

        """

        if self.different_location_duplicate_prob == 0:
            return df

        # Split dataframe by whether the row has been modified during the error-adding process already.
        # Already errored rows do not get further changes.
        df = df.cache()
        previously_modified_rows_df = df.where(F.col("is_modified"))
        unmodified_rows_df = df.where(~(F.col("is_modified")))
        df.unpersist()

        # From unmodified rows, select a subset for adding errors to, depending on config parameter.
        unmodified_rows_df = unmodified_rows_df.cache()
        error_row_prob = self.different_location_duplicate_prob
        unchanged_row_prob = 1.0 - error_row_prob
        unchanged_rows_df, error_rows_df = unmodified_rows_df.randomSplit(
            [unchanged_row_prob, error_row_prob], seed=self.seed + 9
        )

        # Gather rows that are not modified in this step (combine previously-modified rows and not-selected unmodified rows).
        unchanged_rows_df = unchanged_rows_df.union(previously_modified_rows_df)

        # Select all even rows and modify one set of these to offset the location

        error_rows_df = error_rows_df.withColumn(
            "user_row_num",
            F.row_number().over(Window.partitionBy(ColNames.user_id).orderBy(ColNames.timestamp)),
        )

        even_rows = error_rows_df.where(F.col("user_row_num") % 2 == 0).drop("user_row_num")

        # No change to cell id here, as that would additionnally need to check that the modified cell id is actual in the diaries
        modified_rows = even_rows.withColumn(ColNames.latitude, F.col(ColNames.latitude) * F.lit(0.95)).withColumn(
            ColNames.longitude, F.col(ColNames.longitude) * F.lit(0.95)
        )

        error_rows_df = even_rows.union(modified_rows)
        error_rows_df = error_rows_df.withColumn("is_modified", F.lit(True))

        return unchanged_rows_df.union(error_rows_df)

    def generate_errors(self, synth_df_raw: DataFrame) -> DataFrame:
        """
        Inputs a dataframe that contains synthetic records based on diaries.
        These records include locational errors, etc. This function only selects the clean generated records from previous steps.
        Generates errors for those clean records.
        Calls all error generation functions.

        Args:
            synth_df_raw (pyspark.sql.DataFrame): Data of raw and clean synthetic events.

        Returns:
            pyspark.sql.DataFrame: Dataframe, with erroneous records, according to probabilities defined in the configuration.
        """

        synth_df_raw = synth_df_raw.where(~F.col("is_modified"))

        synth_df = synth_df_raw.cache()

        synth_df = self.generate_nulls_in_mandatory_fields(synth_df)
        synth_df = self.generate_out_of_bounds_dates(synth_df)
        synth_df = self.generate_erroneous_type_values(synth_df)
        synth_df = self.generate_same_location_duplicates(synth_df)
        synth_df = self.generate_different_location_duplicates(synth_df)

        return synth_df
