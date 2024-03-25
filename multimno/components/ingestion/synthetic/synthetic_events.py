"""
This module contains the SyntheticEvents class, which is responsible for generating the synthetic event data.
"""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    FloatType,
)
from sedona.sql import st_constructors as STC
from sedona.sql import st_functions as STF
from sedona.sql import st_predicates as STP


from multimno.core.component import Component
from multimno.core.constants.columns import ColNames
from multimno.core.data_objects.bronze.bronze_event_data_object import BronzeEventDataObject
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.core.data_objects.bronze.bronze_synthetic_diaries_data_object import BronzeSyntheticDiariesDataObject
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY


class SyntheticEvents(Component):
    """
    Class that generates the event synthetic data. It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticEvents"

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)

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

    def initalize_data_objects(self):

        pop_diares_input_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "diaries_data_bronze")
        pop_diaries_bronze_event = BronzeSyntheticDiariesDataObject(
            self.spark, pop_diares_input_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        # Input for cell attributes
        network_data_input_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "network_data_bronze")

        cell_locations_bronze = BronzeNetworkDataObject(
            self.spark, network_data_input_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        self.input_data_objects = {
            BronzeSyntheticDiariesDataObject.ID: pop_diaries_bronze_event,
            BronzeNetworkDataObject.ID: cell_locations_bronze,
        }

        output_records_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "event_data_bronze")
        bronze_event = BronzeEventDataObject(
            self.spark, output_records_path, partition_columns=[ColNames.year, ColNames.month, ColNames.day]
        )

        self.output_data_objects = {BronzeEventDataObject.ID: bronze_event}

    def transform(self):

        pop_diaries_df = self.input_data_objects[BronzeSyntheticDiariesDataObject.ID].df
        cells_df = self.input_data_objects[BronzeNetworkDataObject.ID].df.select(
            ColNames.cell_id, ColNames.latitude, ColNames.longitude, ColNames.year, ColNames.month, ColNames.day
        )

        # Filtering stays to get the lat and lon of movement starting and end point
        stays_df = pop_diaries_df.filter(F.col(ColNames.activity_type) == "stay")

        move_events_df = self.generate_event_timestamps_for_moves(stays_df, self.event_freq_moves, self.cartesian_crs)
        move_events_with_locations_df = self.generate_locations_for_moves(move_events_df, self.cartesian_crs)

        stay_events_df = self.generate_event_timestamps_for_stays(stays_df, self.event_freq_stays, self.cartesian_crs)

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
        records_with_cell_id_errors = self.generate_records_with_non_existant_cell_ids(sampled_records, cells_df)

        generated_stays_and_moves = generated_stays_and_moves.subtract(sampled_records)
        generated_stays_and_moves = generated_stays_and_moves.withColumn(
            "closest_cell_distance_max", F.lit(self.closest_cell_distance_max)
        )

        # 3) Link a cell id to each location
        records_sdf = generated_stays_and_moves.union(records_with_location_errors)

        records_sdf = self.add_cell_ids_to_locations(
            records_sdf, cells_df, self.maximum_number_of_cells_for_event, self.seed
        )

        # 4) Continuing with the combined dataframe

        records_sdf = records_sdf.union(records_with_cell_id_errors)

        records_sdf = records_sdf.dropDuplicates([ColNames.user_id, ColNames.timestamp])

        records_sdf = records_sdf.withColumn(
            "generated_geometry",
            STF.ST_Transform("generated_geometry", F.lit(f"EPSG:{self.cartesian_crs}"), F.lit("EPSG:4326")),
        )

        records_sdf = (
            records_sdf.withColumn(ColNames.longitude, STF.ST_X(F.col("generated_geometry")))
            .withColumn(ColNames.latitude, STF.ST_Y(F.col("generated_geometry")))
            .drop("generated_geometry")
        )

        # MCC and loc_error are added to the records
        records_sdf = records_sdf.withColumn(ColNames.mcc, F.lit(self.mcc).cast(IntegerType()))

        # records_sdf = self.calc_hashed_user_id(records_sdf)
        records_sdf = records_sdf.withColumn(ColNames.year, F.year(F.col(ColNames.timestamp)))
        records_sdf = records_sdf.withColumn(ColNames.month, F.month(F.col(ColNames.timestamp)))
        records_sdf = records_sdf.withColumn(ColNames.day, F.dayofmonth(F.col(ColNames.timestamp)))

        # Select bronze schema columns
        columns = {field.name: F.col(field.name).cast(field.dataType) for field in BronzeEventDataObject.SCHEMA.fields}

        records_sdf = records_sdf.withColumns(columns)

        self.output_data_objects[BronzeEventDataObject.ID].df = records_sdf

    @staticmethod
    def generate_event_timestamps_for_moves(
        stays_sdf: DataFrame, event_freq_moves: int, cartesian_crs: int
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
            "next_stay_initial_timestamp", F.lead(ColNames.initial_timestamp, 1).over(window_spec)
        )

        stays_sdf = stays_sdf.withColumn("next_stay_geometry", F.lead(ColNames.geometry, 1).over(window_spec))

        stays_sdf = stays_sdf.withColumn(
            "time_diff_seconds",
            F.unix_timestamp("next_stay_initial_timestamp") - F.unix_timestamp(ColNames.final_timestamp),
        )

        # Calculate how many timestamps fit in the interval for the given frequency
        stays_sdf = stays_sdf.withColumn(
            "timestamps_count", (F.col("time_diff_seconds") / event_freq_moves).cast("integer")
        )

        # Generate random floats between 0 and 1 for, using timestamps_count
        stays_sdf = stays_sdf.withColumn(
            "random_fraction_on_line", F.expr("transform(sequence(1, timestamps_count), x -> rand())")
        ).withColumn("random_fraction_on_line", F.explode(F.col("random_fraction_on_line")))

        # Generate timestamps
        stays_sdf = stays_sdf.withColumn(
            "offset_seconds", F.col("random_fraction_on_line") * F.col("time_diff_seconds")
        )
        stays_sdf = stays_sdf.withColumn(
            ColNames.timestamp, F.from_unixtime(F.unix_timestamp(ColNames.final_timestamp) + F.col("offset_seconds"))
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
        stays_df: DataFrame, event_freq_stays: int, cartesian_crs: int
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
        Returns:
            pyspark.sql.DataFrame: A DataFrame of event timestamps for stays.
        """

        stays_df = stays_df.withColumn(
            "time_diff_seconds",
            F.unix_timestamp(F.col(ColNames.final_timestamp)) - F.unix_timestamp(F.col(ColNames.initial_timestamp)),
        )

        stays_df = stays_df.withColumn(
            "timestamps_count", (F.col("time_diff_seconds") / event_freq_stays).cast("integer")
        )

        stays_df = stays_df.withColumn(
            "random_fraction_between_timestamps", F.expr("transform(sequence(1, timestamps_count), x -> rand())")
        )
        stays_df = stays_df.withColumn(
            "random_fraction_between_timestamps", F.explode(F.col("random_fraction_between_timestamps"))
        )
        stays_df = stays_df.withColumn(
            "offset_seconds", F.col("random_fraction_between_timestamps") * F.col("time_diff_seconds")
        )
        stays_df = stays_df.withColumn(
            ColNames.timestamp, F.from_unixtime(F.unix_timestamp(ColNames.initial_timestamp) + F.col("offset_seconds"))
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
            ColNames.user_id, ColNames.timestamp, "generated_geometry", ColNames.year, ColNames.month, ColNames.day
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
            "line", STF.ST_MakeLine(F.col(ColNames.geometry), F.col("next_stay_geometry"))
        )
        moves_with_geometry = moves_with_geometry.withColumn(
            "generated_geometry",
            STF.ST_SetSRID(STF.ST_LineInterpolatePoint(F.col("line"), F.col("random_fraction_on_line")), cartesian_crs),
        )

        moves_with_geometry = moves_with_geometry.select(
            ColNames.user_id, ColNames.timestamp, "generated_geometry", ColNames.year, ColNames.month, ColNames.day
        )

        return moves_with_geometry

    @staticmethod
    def add_cell_ids_to_locations(
        events_with_locations_df: DataFrame, cells_df: DataFrame, max_n_of_cells: int, seed: int
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
                    STF.ST_Buffer(events_with_locations_df["generated_geometry"], F.col("closest_cell_distance_max")),
                    cells_df["cell_geometry"],
                )
            ),
        ).withColumn(
            "distance_to_cell",
            STF.ST_Distance(events_with_locations_df["generated_geometry"], cells_df["cell_geometry"]),
        )

        # Selection of different cell ids for a given timestamp is random
        window_spec = Window.partitionBy(
            ColNames.year, ColNames.month, ColNames.day, ColNames.user_id, ColNames.timestamp
        ).orderBy(F.col("distance_to_cell"))

        events_with_cells_sdf = events_with_cells_sdf.withColumn(
            "closest_cells_index", F.row_number().over(window_spec)
        ).filter(F.col("closest_cells_index") <= max_n_of_cells)

        window_spec_random = Window.partitionBy(
            ColNames.year, ColNames.month, ColNames.day, ColNames.user_id, ColNames.timestamp
        ).orderBy(F.rand(seed))
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
            (F.rand(seed) * (error_location_distance_max - error_location_distance_min)) + error_location_distance_min,
        )

        errors_df = (
            errors_df.withColumn("sign", F.when(F.rand(seed) > 0.5, 1).otherwise(-1))
            .withColumn("new_x", F.col("x") + (F.col(ColNames.loc_error) * F.col("sign")))
            .withColumn("new_y", F.col("y") + (F.col(ColNames.loc_error) * F.col("sign")))
        )

        errors_df = errors_df.withColumn(
            "generated_geometry", STF.ST_SetSRID(STC.ST_Point(F.col("new_x"), F.col("new_y")), cartesian_crs)
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
    def generate_records_with_non_existant_cell_ids(records_sdf: DataFrame, cells_sdf: DataFrame) -> DataFrame:
        """
        Adds the cell_id column so that it will contain cell_ids that
        are not present in the cells_df dataframe, yet follow the format of a cell id.

        Args:
            records_sdf (DataFrame): generated records
            cells_sdf (DataFrame): cells dataframe

        Returns:
            DataFrame: records with cell ids that are not present in the cells_df dataframe
        """

        # Generates random cell ids for cells_df, and selects those
        # Join to records is implemented with a monotonically increasing id
        # So to limit that, this number of all unique cells is used
        # TODO check how to make this more optimal

        n_of_actual_cell_ids_as_base = cells_sdf.count()

        cells_df_with_random_cell_ids = cells_sdf[[ColNames.cell_id]].withColumn(
            "random_cell_id",
            (F.rand() * (999_999_999_999_999 - 10_000_000_000_000) + 10_000_000_000_000).cast("bigint"),
        )

        # Do an  left anti join to ensure that now generated cell ids are not among actual cell ids

        cells_df_inner_joined = cells_df_with_random_cell_ids[["random_cell_id"]].join(
            cells_sdf[[ColNames.cell_id]], on=F.col("random_cell_id") == F.col(ColNames.cell_id), how="leftanti"
        )

        cells_df_inner_joined = cells_df_inner_joined.withColumn("row_number", (F.monotonically_increasing_id()))
        records_sdf = records_sdf.withColumn(
            "row_number", (F.monotonically_increasing_id() % n_of_actual_cell_ids_as_base) + 1
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

    @staticmethod
    def calc_hashed_user_id(sdf) -> DataFrame:
        """
        Calculates SHA2 hash of user id, takes the first 31 bits and converts them to a non-negative 32-bit integer.

        Args:
            sdf (pyspark.sql.DataFrame): Data of clean synthetic events with a user id column.

        Returns:
            pyspark.sql.DataFrame: Dataframe, where user_id column is transformered to a hashed value.

        """

        sdf = sdf.withColumn("hashed_user_id", F.sha2(sdf[ColNames.user_id].cast("string"), 256))
        sdf = sdf.withColumn(ColNames.user_id, F.unhex(F.col("hashed_user_id")))
        sdf = sdf.drop("hashed_user_id")

        return sdf
