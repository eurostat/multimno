"""
Module that generates a MNO synthetic network.
"""

from random import Random
import datetime
from abc import abstractmethod, ABCMeta
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType, IntegerType, StringType, LongType
from typing import Union, List

from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_network_physical_data_object import BronzeNetworkDataObject
from multimno.core.constants.columns import ColNames


class CellIDGenerator(metaclass=ABCMeta):
    """
    Abstract class for cell ID generation.
    """

    def __init__(self, rng: Union[int, Random]) -> None:
        """Cell ID Generator constructor

        Args:
            rng (Union[int, Random]): either an integer to act as a seed for RNG, or an instantiated Random object.
        """
        if isinstance(rng, int):
            self.rng = Random(rng)
        elif isinstance(rng, Random):
            self.rng = rng

    @abstractmethod
    def generate_cell_ids(self, n_cells: int) -> List[str]:
        """Method that generates random cell IDs.

        Args:
            n_cells (int): number of cell IDs to generate.

        Returns:
            List[str]: list of cell IDs.
        """
        return None


class RandomCellIDGenerator(CellIDGenerator):
    """
    Class that generates completely random cell IDs. Inherits from the AbstractCellIDGenerator class.
    """

    def generate_cell_ids(self, n_cells: int) -> List[str]:
        """Generate UNIQUE random cell IDs with no logic behind it, i.e. not following CGI/eCGI standards.
        The resuling cell IDs are 14- or 15-digit strings.

        Args:
            n_cells (int): number of cell IDs to generate.

        Returns:
            List[str]: list of cell IDs.
        """
        return list(map(str, self.rng.sample(range(10_000_000_000_000, 999_999_999_999_999), n_cells)))


class CellIDGeneratorBuilder:
    """
    Type/method of cell ID generation enumeration class.
    """

    RANDOM_CELL_ID = "random_cell_id"

    CONSTRUCTORS = {RANDOM_CELL_ID: RandomCellIDGenerator}

    @staticmethod
    def build(constructor_key: str, rng: Union[int, Random]) -> CellIDGenerator:
        """
        Method that builds a CellIDGenerator.

        Args:
            constructor_key (str): Key of the constructor
            rng (Union[int, Random]): either an integer to act as a seed for RNG, or an instantiated Random object.

        Raises:
            ValueError: If the given constructor_key is not supported

        Returns:
            CellIDGenerator: Class that generates random cell_id's
        """
        try:
            constructor = CellIDGeneratorBuilder.CONSTRUCTORS[constructor_key]
        except KeyError as e:
            raise ValueError(f"Random cell ID generator: {constructor_key} is not supported") from e

        return constructor(rng)


class SyntheticNetwork(Component):
    """
    Class that generates the synthetic network topology data. It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticNetwork"

    def __init__(self, general_config_path: str, component_config_path: str):
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)
        self.seed = self.config.getint(self.COMPONENT_ID, "seed")
        self.rng = Random(self.seed)
        self.n_cells = self.config.getint(self.COMPONENT_ID, "n_cells")
        self.cell_type_options = (
            self.config.get(self.COMPONENT_ID, "cell_type_options").strip().replace(" ", "").split(",")
        )
        self.tech = ["5G", "LTE", "UMTS", "GSM"]
        self.latitude_min = self.config.getfloat(self.COMPONENT_ID, "latitude_min")
        self.latitude_max = self.config.getfloat(self.COMPONENT_ID, "latitude_max")
        self.longitude_min = self.config.getfloat(self.COMPONENT_ID, "longitude_min")
        self.longitude_max = self.config.getfloat(self.COMPONENT_ID, "longitude_max")
        self.altitude_min = self.config.getfloat(self.COMPONENT_ID, "altitude_min")
        self.altitude_max = self.config.getfloat(self.COMPONENT_ID, "altitude_max")
        self.antenna_height_max = self.config.getfloat(self.COMPONENT_ID, "antenna_height_max")
        self.power_min = self.config.getfloat(self.COMPONENT_ID, "power_min")
        self.power_max = self.config.getfloat(self.COMPONENT_ID, "power_max")
        self.range_min = self.config.getfloat(self.COMPONENT_ID, "range_min")
        self.range_max = self.config.getfloat(self.COMPONENT_ID, "range_max")
        self.frequency_min = self.config.getfloat(self.COMPONENT_ID, "frequency_min")
        self.frequency_max = self.config.getfloat(self.COMPONENT_ID, "frequency_max")

        self.timestamp_format = self.config.get(self.COMPONENT_ID, "timestamp_format")
        self.earliest_valid_date_start = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "earliest_valid_date_start"), self.timestamp_format
        )
        self.latest_valid_date_end = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "latest_valid_date_end"), self.timestamp_format
        )

        self.date_format = self.config.get(self.COMPONENT_ID, "date_format")

        self.starting_date = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "starting_date"), self.date_format
        ).date()
        self.ending_date = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "ending_date"), self.date_format
        ).date()

        self.date_range = [
            (self.starting_date + datetime.timedelta(days=dd))
            for dd in range((self.ending_date - self.starting_date).days + 1)
        ]

        # Cell generation object
        cell_id_generation_type = self.config.get(self.COMPONENT_ID, "cell_id_generation_type")

        self.cell_id_generator = CellIDGeneratorBuilder.build(cell_id_generation_type, self.seed)

        self.no_optional_fields_probability = self.config.getfloat(self.COMPONENT_ID, "no_optional_fields_probability")
        self.mandatory_null_probability = self.config.getfloat(self.COMPONENT_ID, "mandatory_null_probability")
        self.out_of_bounds_values_probability = self.config.getfloat(
            self.COMPONENT_ID, "out_of_bounds_values_probability"
        )
        self.erroneous_values_probability = self.config.getfloat(self.COMPONENT_ID, "erroneous_values_probability")

    def initalize_data_objects(self):
        output_network_data_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "network_data_bronze")
        bronze_network = BronzeNetworkDataObject(self.spark, output_network_data_path)
        self.output_data_objects = {bronze_network.ID: bronze_network}

    def read(self):
        pass  # No input datasets are used in this component

    def transform(self):
        spark = self.spark

        # Create Spark DataFrame with all valid cells and all optional fields

        cells_df = spark.createDataFrame(self.clean_cells_generator(), schema=BronzeNetworkDataObject.SCHEMA)

        # With certain probability, set ALL optional fields of a row to null.
        # Fixing F.rand(seed=self.seed) will generate the same random column for every column, so
        # it could be optimized to be generated only once
        # If random optional fields should be set to zero (not all at the same time), use seed = self.seed + i(col_name)

        for col_name in BronzeNetworkDataObject.OPTIONAL_COLUMNS:
            cells_df = cells_df.withColumn(
                col_name,
                F.when(F.rand(seed=self.seed) < self.no_optional_fields_probability, None).otherwise(F.col(col_name)),
            )

        cells_df = self.generate_errors(cells_df)

        self.output_data_objects[BronzeNetworkDataObject.ID].df = cells_df

    def clean_cells_generator(self):
        """Method that generates valid and fully complete (mandatory and optional) cell physical attibutes.

        An underlying set of cells are created, covering the config-specified date interval.
        Then, for each cell and date,
        the valid_date_start and valid_date_end fields, marking the time interval in which the cell is operational, are
        comparted with the date:
            a) If  date < valid_date_start, the cell-date row will not appear.
            b) If valid_date_start <= date < valid_date_end, the cell-date row will appear and
                the valid_date_end will be null, as the cell was currently operational
            c) If valid_date_end <= date, the cell-date row will appear and
                the valid_date_end will NOT be null, marking the past, now known, time interval of operation.

        Yields:
            (
                cell_id (str),
                latitude (float),
                longitude (float),
                altitudes (float),
                antenna_height (float),
                directionality (int),
                azimuth_angle (float | None),
                elevation_angle (float),
                hor_beam_width (float),
                ver_beam_width (float),
                power (float),
                range (float),
                frequency (int),
                technology (str),
                valid_date_start (str),
                valid_date_end (str | None)
                cell_type (str),
                year (int),
                month (int),
                day (int)
            )
        """
        # MANDATORY FIELDS
        cell_ids = self.cell_id_generator.generate_cell_ids(self.n_cells)

        latitudes = [self.rng.uniform(self.latitude_min, self.latitude_max) for _ in range(self.n_cells)]
        longitudes = [self.rng.uniform(self.longitude_min, self.longitude_max) for _ in range(self.n_cells)]

        # OPTIONAL FIELDS
        altitudes = [self.rng.uniform(self.altitude_min, self.altitude_max) for _ in range(self.n_cells)]

        # antenna height always positive
        antenna_heights = [self.rng.uniform(0, self.antenna_height_max) for _ in range(self.n_cells)]

        # Directionality: 0 or 1
        directionalities = [self.rng.choice([0, 1]) for _ in range(self.n_cells)]

        def random_azimuth_angle(directionality):
            if directionality == 0:
                return None
            else:
                return self.rng.uniform(0, 360)

        # Azimuth angle: None if directionality is 0, float in [0, 360] if directionality is 1
        azimuth_angles = [random_azimuth_angle(direc) for direc in directionalities]

        # Eleveation angle: in [-90, 90]
        elevation_angles = [self.rng.uniform(-90, 90) for _ in range(self.n_cells)]

        # Horizontal/Vertical beam width: float in [0, 360]
        hor_beam_widths = [self.rng.uniform(0, 360) for _ in range(self.n_cells)]
        ver_beam_widths = [self.rng.uniform(0, 360) for _ in range(self.n_cells)]

        # Power, float in specified range (unit: watts, W)
        powers = [self.rng.uniform(self.power_min, self.power_max) for _ in range(self.n_cells)]

        # Range, float in specified range (unit: metres, m)
        ranges = [self.rng.uniform(self.range_min, self.range_max) for _ in range(self.n_cells)]

        # Frequency: int in specifed range (unit: MHz)
        frequencies = [self.rng.randint(self.frequency_min, self.frequency_max) for _ in range(self.n_cells)]

        # Technology: str
        technologies = [self.rng.choice(self.tech) for _ in range(self.n_cells)]

        # # Valid start date, should be in the timestamp interval provided via config file
        # span_seconds = (self.latest_valid_date_end - self.earliest_valid_date_start).total_seconds()

        # # Start date will be some random nb of seconds after the earliest valid date start
        # valid_date_start_dts = [
        #     self.earliest_valid_date_start + datetime.timedelta(seconds=self.rng.uniform(0, span_seconds))
        #     for _ in range(self.n_cells)
        # ]

        # # Remaining seconds from the valid date starts to the ending date
        # remaining_seconds = [
        #     (self.latest_valid_date_end - start_dt).total_seconds() for start_dt in valid_date_start_dts
        # ]
        # # Choose valid date ends ALWAYS after the valid date start
        # # Minimum of two seconds, as valid date end is excluded from the time window,
        # # so cell will be valid for at least 1 second
        # valid_date_end_dts = [
        #     (start_dt + datetime.timedelta(seconds=self.rng.uniform(2, rem_secs)))
        #     for (start_dt, rem_secs) in zip(valid_date_start_dts, remaining_seconds)
        # ]

        valid_date_start_dts = [self.earliest_valid_date_start for _ in range(self.n_cells)]
        valid_date_end_dts = [self.latest_valid_date_end for _ in range(self.n_cells)]

        def check_operational(_curr_date: datetime.date, _end_datetime: datetime.datetime):
            if _curr_date < _end_datetime.date():  # still operational, return None
                return None
            else:
                return _end_datetime.strftime(self.timestamp_format)

        # Cell type: str in option list
        cell_types = [self.rng.choice(self.cell_type_options) for _ in range(self.n_cells)]

        for date in self.date_range:
            for i in range(self.n_cells):
                # Assume we do not have info about future cells
                if valid_date_start_dts[i].date() <= date:
                    yield (
                        cell_ids[i],
                        latitudes[i],
                        longitudes[i],
                        altitudes[i],
                        antenna_heights[i],
                        directionalities[i],
                        azimuth_angles[i],
                        elevation_angles[i],
                        hor_beam_widths[i],
                        ver_beam_widths[i],
                        powers[i],
                        ranges[i],
                        frequencies[i],
                        technologies[i],
                        valid_date_start_dts[i].strftime(self.timestamp_format),
                        check_operational(date, valid_date_end_dts[i]),  # null if still operational today
                        cell_types[i],
                        date.year,
                        date.month,
                        date.day,
                    )

    def generate_errors(self, df: DataFrame) -> DataFrame:
        """Function handling the generation of out-of-bounds, null and erroneous values in different columns
        according to the config-specified probabilities

        Args:
            df (DataFrame): clean DataFrame

        Returns:
            DataFrame: DataFrame after the generation of different invalid or null values
        """

        if self.out_of_bounds_values_probability > 0:
            df = self.generate_out_of_bounds_values(df)

        if self.mandatory_null_probability > 0:
            df = self.generate_nulls_in_mandatory_columns(df)

        if self.erroneous_values_probability > 0:
            df = self.generate_erroneous_values(df)

        return df

    def generate_nulls_in_mandatory_columns(self, df: DataFrame) -> DataFrame:
        """Generates null values in the mandatory fields based on probabilities form config

        Args:
            df (DataFrame): synthetic dataframe

        Returns:
            DataFrame: synthetic dataframe with nulls in some mandatory fields
        """
        # Use different seed for each column
        for i, col_name in enumerate(BronzeNetworkDataObject.MANDATORY_COLUMNS):
            df = df.withColumn(
                col_name,
                F.when(F.rand(seed=self.seed + i + 1) < self.mandatory_null_probability, None).otherwise(
                    F.col(col_name)
                ),
            )

        return df

    def generate_out_of_bounds_values(self, df: DataFrame) -> DataFrame:
        """Function that generates out-of-bounds values for the appropriate columns of the data object

        Args:
            df (DataFrame): cell dataframe with in-bound values

        Returns:
            DataFrame: cell dataframe with some out-of-bounds values
        """
        # (2 * F.round(F.rand(seed=self.seed-1)) - 1) -> random column of 1s and -1s
        df = df.withColumn(
            ColNames.latitude,
            F.when(
                F.rand(seed=self.seed - 1) < self.out_of_bounds_values_probability,
                F.col(ColNames.latitude) + (2 * F.round(F.rand(seed=self.seed - 1)) - 1) * 90,
            )
            .otherwise(F.col(ColNames.latitude))
            .cast(FloatType()),  # the operation turns it into DoubleType, so cast it to Float
        )

        df = df.withColumn(
            ColNames.longitude,
            F.when(
                F.rand(seed=self.seed - 2) < self.out_of_bounds_values_probability,
                F.col(ColNames.longitude) + (2 * F.round(F.rand(seed=self.seed - 2)) - 1) * 180,
            )
            .otherwise(F.col(ColNames.longitude))
            .cast(FloatType()),
        )

        # antenna height, non positive
        df = df.withColumn(
            ColNames.antenna_height,
            F.when(F.rand(seed=self.seed - 3) < self.out_of_bounds_values_probability, -F.col(ColNames.antenna_height))
            .otherwise(F.col(ColNames.antenna_height))
            .cast(FloatType()),
        )

        # directionality: int different from 0 or 1. Just add a static 5 to the value
        df = df.withColumn(
            ColNames.directionality,
            F.when(
                F.rand(seed=self.seed - 4) < self.out_of_bounds_values_probability, F.col(ColNames.directionality) + 5
            )
            .otherwise(F.col(ColNames.directionality))
            .cast(IntegerType()),
        )

        # azimuth angle: outside of [0, 360]
        df = df.withColumn(
            ColNames.azimuth_angle,
            F.when(
                F.rand(seed=self.seed - 5) < self.out_of_bounds_values_probability,
                F.col(ColNames.azimuth_angle) + (2 * F.round(F.rand(seed=self.seed - 5)) - 1) * 360,
            )
            .otherwise(F.col(ColNames.azimuth_angle))
            .cast(FloatType()),
        )

        # elevation_angle: outside of [-90, 90]
        df = df.withColumn(
            ColNames.elevation_angle,
            F.when(
                F.rand(seed=self.seed - 6) < self.out_of_bounds_values_probability,
                F.col(ColNames.elevation_angle) + (2 * F.round(F.rand(seed=self.seed - 6)) - 1) * 180,
            )
            .otherwise(F.col(ColNames.elevation_angle))
            .cast(FloatType()),
        )

        # horizontal_beam_width: outside of [0, 360]
        df = df.withColumn(
            ColNames.horizontal_beam_width,
            F.when(
                F.rand(seed=self.seed - 7) < self.out_of_bounds_values_probability,
                F.col(ColNames.horizontal_beam_width) + (2 * F.round(F.rand(seed=self.seed - 7)) - 1) * 360,
            )
            .otherwise(F.col(ColNames.horizontal_beam_width))
            .cast(FloatType()),
        )

        # vertical_beam_width: outside of [0, 360]
        df = df.withColumn(
            ColNames.vertical_beam_width,
            F.when(
                F.rand(seed=self.seed - 8) < self.out_of_bounds_values_probability,
                F.col(ColNames.vertical_beam_width) + (2 * F.round(F.rand(seed=self.seed - 8)) - 1) * 360,
            )
            .otherwise(F.col(ColNames.vertical_beam_width))
            .cast(FloatType()),
        )

        # power: non positive value
        df = df.withColumn(
            ColNames.power,
            F.when(F.rand(seed=self.seed - 9) < self.out_of_bounds_values_probability, -F.col(ColNames.power))
            .otherwise(F.col(ColNames.power))
            .cast(FloatType()),
        )

        # range: non positive value
        df = df.withColumn(
            ColNames.range,
            F.when(F.rand(seed=self.seed - 10) < self.out_of_bounds_values_probability, -F.col(ColNames.range))
            .otherwise(F.col(ColNames.range))
            .cast(FloatType()),
        )

        # frequency: non positive vallue
        df = df.withColumn(
            ColNames.frequency,
            F.when(F.rand(seed=self.seed - 11) < self.out_of_bounds_values_probability, -F.col(ColNames.frequency))
            .otherwise(F.col(ColNames.frequency))
            .cast(IntegerType()),
        )
        return df

    def generate_erroneous_values(self, df: DataFrame) -> DataFrame:
        """Function that generates erroneous values in the cell_id, valid_date_start and valid_date_end columns

        Args:
            df (DataFrame): DataFrame before the generation of erroneous values

        Returns:
            DataFrame: DataFrame with erroneous values
        """
        # Erroneous cells: for now, a string not of 14 or 15 digits
        df = df.withColumn(
            ColNames.cell_id,
            F.when(
                F.rand(seed=self.seed * 1000) < self.erroneous_values_probability,
                (
                    F.when(
                        F.rand(seed=self.seed * 2000) > 0.5,
                        F.concat(
                            F.col(ColNames.cell_id), F.substring(F.col(ColNames.cell_id), 3, 3)
                        ),  # 17 or 18 digits
                    ).otherwise(
                        F.substring(F.col(ColNames.cell_id), 3, 10)  # 10 digits
                    )
                ),
            )
            .otherwise(F.col(ColNames.cell_id))
            .cast(LongType())
            .cast(StringType()),
        )

        # Dates
        df_as_is, df_swap, df_wrong = df.randomSplit(
            weights=[
                1 - self.erroneous_values_probability,
                self.erroneous_values_probability / 2,
                self.erroneous_values_probability / 2,
            ],
            seed=self.seed,
        )
        # For some columns, swap valid_date_start and valid_date_end
        df_swap = df_swap.withColumns(
            {ColNames.valid_date_start: ColNames.valid_date_end, ColNames.valid_date_end: ColNames.valid_date_start}
        )

        chars_to_change = ["0", "T", ":", "/", "2"]
        changed_chars = ["0", "T", ":", "/", "2"]
        # Now, for dates as well, make the timestamp format incorrect

        self.rng.shuffle(changed_chars)
        df_wrong = df_wrong.withColumn(
            ColNames.valid_date_start,
            F.translate(F.col(ColNames.valid_date_start), "".join(chars_to_change), "".join(changed_chars)),
        )

        self.rng.shuffle(changed_chars)
        df_wrong = df_wrong.withColumn(
            ColNames.valid_date_end,
            F.translate(F.col(ColNames.valid_date_end), "".join(chars_to_change), "".join(changed_chars)),
        )

        df = df_as_is.union(df_swap).union(df_wrong)

        return df
