"""
Module that generates a MNO synthetic network.
"""

import datetime
from math import sqrt, pi, cos, sin, asin, radians
from typing import Any
from random import Random
from pyspark.sql import Row
import pyspark.sql.functions as F

from multimno.core.utils import calc_hashed_user_id
from multimno.core.settings import CONFIG_BRONZE_PATHS_KEY
from multimno.core.component import Component
from multimno.core.data_objects.bronze.bronze_synthetic_diaries_data_object import BronzeSyntheticDiariesDataObject
from multimno.core.constants.columns import ColNames


class SyntheticDiaries(Component):
    """
    Class that generates the synthetic activity-trip diaries data.
    It inherits from the Component abstract class.
    """

    COMPONENT_ID = "SyntheticDiaries"

    def __init__(self, general_config_path: str, component_config_path: str):
        # keep super class init method:
        super().__init__(general_config_path=general_config_path, component_config_path=component_config_path)

        # and additionally:
        # self.n_partitions = self.config.getint(self.COMPONENT_ID, "n_partitions")

        self.number_of_users = self.config.getint(self.COMPONENT_ID, "number_of_users")

        self.date_format = self.config.get(self.COMPONENT_ID, "date_format")
        self.initial_date = datetime.datetime.strptime(
            self.config.get(self.COMPONENT_ID, "initial_date"), self.date_format
        ).date()
        self.number_of_dates = self.config.getint(self.COMPONENT_ID, "number_of_dates")
        self.date_range = [(self.initial_date + datetime.timedelta(days=d)) for d in range(self.number_of_dates)]

        self.longitude_min = self.config.getfloat(self.COMPONENT_ID, "longitude_min")
        self.longitude_max = self.config.getfloat(self.COMPONENT_ID, "longitude_max")
        self.latitude_min = self.config.getfloat(self.COMPONENT_ID, "latitude_min")
        self.latitude_max = self.config.getfloat(self.COMPONENT_ID, "latitude_max")

        self.home_work_distance_min = self.config.getfloat(self.COMPONENT_ID, "home_work_distance_min")
        self.home_work_distance_max = self.config.getfloat(self.COMPONENT_ID, "home_work_distance_max")
        self.other_distance_min = self.config.getfloat(self.COMPONENT_ID, "other_distance_min")
        self.other_distance_max = self.config.getfloat(self.COMPONENT_ID, "other_distance_max")

        self.home_duration_min = self.config.getfloat(self.COMPONENT_ID, "home_duration_min")
        self.home_duration_max = self.config.getfloat(self.COMPONENT_ID, "home_duration_max")
        self.work_duration_min = self.config.getfloat(self.COMPONENT_ID, "work_duration_min")
        self.work_duration_max = self.config.getfloat(self.COMPONENT_ID, "work_duration_max")
        self.other_duration_min = self.config.getfloat(self.COMPONENT_ID, "other_duration_min")
        self.other_duration_max = self.config.getfloat(self.COMPONENT_ID, "other_duration_max")

        self.displacement_speed = self.config.getfloat(self.COMPONENT_ID, "displacement_speed")

        self.stay_sequence_superset = self.config.get(self.COMPONENT_ID, "stay_sequence_superset").split(",")
        self.stay_sequence_probabilities = [
            float(w)
            for w in self.config.get(self.COMPONENT_ID, "stay_sequence_probabilities").split(
                ","
            )  # TODO: cambiar por stay_sequence
        ]
        assert len(self.stay_sequence_superset) == len(self.stay_sequence_probabilities)

    def initalize_data_objects(self):
        output_synthetic_diaries_data_path = self.config.get(CONFIG_BRONZE_PATHS_KEY, "diaries_data_bronze")
        bronze_synthetic_diaries = BronzeSyntheticDiariesDataObject(
            self.spark,
            output_synthetic_diaries_data_path,
            partition_columns=[ColNames.year, ColNames.month, ColNames.day],
        )
        self.output_data_objects = {BronzeSyntheticDiariesDataObject.ID: bronze_synthetic_diaries}

    def read(self):
        pass  # No input datasets are used in this component

    def transform(self):
        spark = self.spark
        activities_df = spark.createDataFrame(self.generate_activities())
        activities_df = calc_hashed_user_id(activities_df)
        columns = {
            field.name: F.col(field.name).cast(field.dataType)
            for field in BronzeSyntheticDiariesDataObject.SCHEMA.fields
        }
        activities_df = activities_df.withColumns(columns)
        self.output_data_objects[BronzeSyntheticDiariesDataObject.ID].df = activities_df

    def haversine(self, lon1: float, lat1: float, lon2: float, lat2: float) -> float:
        """
        Calculate the haversine distance in meters between two points.

        Args:
            lon1 (float): longitude of first point, in decimal degrees.
            lat1 (float): latitude of first point, in decimal degrees.
            lon2 (float): longitude of second point, in decimal degrees.
            lat2 (float): latitude of second point, in decimal degrees.

        Returns:
            float: distance between both points, in meters.
        """
        r = 6_371_000  # Radius of earth in meters.

        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return c * r

    def random_seed_number_generator(
        self, base_seed: int, agent_id: int = None, date: datetime.date = None, i: int = None
    ) -> int:
        """
        Generate random seed integer based on provided arguments.

        Args:
            base_seed (int): base integer for operations.
            agent_id (int, optional): agent identifier. Defaults to None.
            date (datetime.date, optional): date. Defaults to None.
            i (int, optional): position integer. Defaults to None.

        Returns:
            int: generated random seed integer.
        """
        seed = base_seed
        if agent_id is not None:
            seed += int(agent_id) * 100
        if date is not None:
            start_datetime = datetime.datetime.combine(date, datetime.time(0))
            seed += int(start_datetime.timestamp())
        if i is not None:
            seed += i
        return seed

    def calculate_trip_time(self, o_location: tuple[float, float], d_location: tuple[float, float]) -> float:
        """
        Calculate trip time given an origin location and a destination
        location, according to the specified trip speed.

        Args:
            o_location (tuple[float,float]): lon, lat of 1st point,
                in decimal degrees.
            d_location (tuple[float,float]): lon, lat of 2nd point,
                in decimal degrees.

        Returns:
            float: trip time, in seconds.
        """
        trip_distance = self.haversine(o_location[0], o_location[1], d_location[0], d_location[1])  # m
        trip_speed = self.displacement_speed  # m/s
        trip_time = trip_distance / trip_speed  # s
        return trip_time

    def calculate_trip_final_time(
        self,
        origin_location: tuple[float, float],
        destin_location: tuple[float, float],
        origin_timestamp: datetime.datetime,
    ) -> datetime.datetime:
        """
        Calculate end time of a trip given an origin time, an origin location,
        a destination location and a speed.

        Args:
            origin_location (tuple[float,float]): lon, lat of 1st point,
                in decimal degrees.
            destin_location (tuple[float,float]): lon, lat of 2nd point,
                in decimal degrees.
            origin_timestamp (datetime.datetime): start time of trip.

        Returns:
            datetime.datetime: end time of trip.
        """

        trip_time = self.calculate_trip_time(origin_location, destin_location)  # s
        return origin_timestamp + datetime.timedelta(seconds=trip_time)

    def generate_stay_location(
        self,
        stay_type: str,
        home_location: tuple[float, float],
        work_location: tuple[float, float],
        previous_location: tuple[float, float],
        user_id: int,
        date: datetime.date,
        i: int,
    ) -> tuple[float, float]:
        """
        Generate a random activity location within the bounding box limits based
        on the activity type and previous activity locations.

        Args:
            stay_type (str): type of stay ("home", "work" or "other").
            home_location (tuple[float,float]): coordinates of home location.
            work_location (tuple[float,float]): coordinates of work location.
            previous_location (tuple[float,float]): coordinates of previous
                activity location.
            user_id (int): agent identifier, used for random seed generation.
            date (datetime.date): date, used for random seed generation.
            i (int): activity position, used for random seed generation.

        Returns:
            tuple[float,float]: randomly generated activity location coordinates.
        """
        if stay_type == "home":
            location = home_location
        elif stay_type == "work":
            location = work_location
        else:
            location = self.generate_other_location(user_id, date, i, home_location, previous_location)
        return location

    def create_agent_activities_min_duration(
        self,
        user_id: int,
        agent_stay_type_sequence: list[str],
        home_location: tuple[float, float],
        work_location: tuple[float, float],
        date: datetime.date,
        start_of_date: datetime.datetime,
        end_of_date: datetime.datetime,
    ) -> list[Row]:
        """
        Generate activities of the minimum duration following the specified agent
        activity sequence for this agent and date.

        Args:
            user_id (int): agent identifier.
            agent_stay_type_sequence (list[str]): list of generated stay types,
                each represented by a string indicating the stay type.
            home_location (tuple[float,float]): coordinates of home location.
            work_location (tuple[float,float]): coordinates of work location.
            date (datetime.date): date for activity sequence generation, used for
                timestamps and random seed generation.
            start_of_date (datetime.datetime): timestamp of current date at 00:00:00.
            end_of_date (datetime.datetime): timestamp of current date at 23:59:59.

        Returns:
            list[Row]: list of generated activities and trips, each represented by a
                spark row object with all its information.
        """
        date_activities = []
        previous_location = None
        for i, stay_type in enumerate(agent_stay_type_sequence):
            # activity location:
            location = self.generate_stay_location(
                stay_type, home_location, work_location, previous_location, user_id, date, i
            )
            # previous move (unless first stay)
            if i != 0:
                # move timestamps:
                trip_initial_timestamp = stay_final_timestamp
                trip_final_timestamp = self.calculate_trip_final_time(
                    previous_location, location, trip_initial_timestamp
                )
                # add move:
                date_activities.append(
                    Row(
                        user_id=user_id,
                        activity_type="move",
                        stay_type="move",
                        longitude=float("nan"),
                        latitude=float("nan"),
                        initial_timestamp=trip_initial_timestamp,
                        final_timestamp=trip_final_timestamp,
                        year=date.year,
                        month=date.month,
                        day=date.day,
                    )
                )
            # stay timestamps:
            stay_initial_timestamp = start_of_date if i == 0 else trip_final_timestamp
            stay_duration = self.generate_min_stay_duration(stay_type)
            stay_final_timestamp = stay_initial_timestamp + datetime.timedelta(hours=stay_duration)

            # add stay:
            date_activities.append(
                Row(
                    user_id=user_id,
                    activity_type="stay",
                    stay_type=stay_type,
                    longitude=location[0],
                    latitude=location[1],
                    initial_timestamp=stay_initial_timestamp,
                    final_timestamp=stay_final_timestamp,
                    year=date.year,
                    month=date.month,
                    day=date.day,
                )
            )

            previous_location = location

        # after the iterations:
        if not date_activities:  # 0 stays
            condition_for_full_home = True
        elif stay_final_timestamp > end_of_date:  # too many stays
            condition_for_full_home = True
        else:
            condition_for_full_home = False

        if condition_for_full_home:  # simple "only home" diary
            return [
                Row(
                    user_id=user_id,
                    activity_type="stay",
                    stay_type="home",
                    longitude=home_location[0],
                    latitude=home_location[1],
                    initial_timestamp=start_of_date,
                    final_timestamp=end_of_date,
                    year=date.year,
                    month=date.month,
                    day=date.day,
                )
            ]
        else:
            return date_activities  # actual generated diary

    def update_spark_row(self, row: Row, column_name: str, new_value: Any) -> Row:
        """
        Return an updated spark row object, changing the value of a column.

        Args:
            row (Row): input spark row.
            column_name (str): name of column to modify.
            new_value (Any): new value to assign.

        Returns:
            Row: modified spark row
        """
        return Row(**{**row.asDict(), **{column_name: new_value}})

    def adjust_activity_times(
        self,
        date_activities: list[Row],
        remaining_time: float,
        user_id: int,
        date: datetime.date,
        start_of_date: datetime.datetime,
        end_of_date: datetime.datetime,
    ):
        """
        Modifies the "date_activities" list, changing the initial and
        final timestamps of both stays and moves probablilistically in order to
        generate stay durations different from the minimum and adjust the
        durations of the activities to the 24h of the day.

        Args:
            date_activities (list[Row]): list of generated activities (stays and
                moves) of the agent for the specified date. Each activity/trip is a
                spark row object.
            user_id (int): agent identifier.
            date (datetime.date): date for activity sequence generation, used for
                timestamps and random seed generation.
            start_of_date (datetime.datetime): timestamp of current date at 00:00:00.
            end_of_date (datetime.datetime): timestamp of current date at 23:59:59.
        """
        current_timestamp = start_of_date
        for i, activity_row in enumerate(date_activities):
            if activity_row.activity_type == "stay":  # stay:
                stay_type = activity_row.stay_type
                old_stay_duration = (
                    activity_row.final_timestamp - activity_row.initial_timestamp
                ).total_seconds() / 3600.0
                new_initial_timestamp = current_timestamp
                if i == len(date_activities) - 1:
                    new_final_timestamp = end_of_date
                    remaining_time = 0.0
                else:
                    new_stay_duration = self.generate_stay_duration(user_id, date, i, stay_type, remaining_time)
                    new_duration_td = datetime.timedelta(seconds=new_stay_duration * 3600.0)
                    new_final_timestamp = new_initial_timestamp + new_duration_td
                    remaining_time -= new_stay_duration - old_stay_duration
            else:  # move:
                old_move_duration = activity_row.final_timestamp - activity_row.initial_timestamp
                new_initial_timestamp = current_timestamp
                new_final_timestamp = new_initial_timestamp + old_move_duration

            # common for all activities (stays and moves):
            activity_row = self.update_spark_row(activity_row, "initial_timestamp", new_initial_timestamp)
            activity_row = self.update_spark_row(activity_row, "final_timestamp", new_final_timestamp)
            date_activities[i] = activity_row
            current_timestamp = new_final_timestamp

    def add_agent_date_activities(
        self,
        activities: list[Row],
        user_id: int,
        agent_stay_type_sequence: list[str],
        home_location: tuple[float, float],
        work_location: tuple[float, float],
        date: datetime.date,
        start_of_date: datetime.datetime,
        end_of_date: datetime.datetime,
    ):
        """
        For a specific date and user, generate a sequence of activities probabilistically
        according to the specified activity superset and the activity probabilities.
        Firstly, assign to each of these activities the minimum duration considered for
        that activity type. Trip times are based on Pythagorean distance and a specified
        average speed.
        If the sum of all minimum duration of the activities and the duration of the trips
        is higher than the 24h of the day, then assign just one "home" activity to the
        agent from 00:00:00 to 23:59:59.
        Else, there will be a remaining time. E.g., the diary of an agent, after adding
        up all trip durations and minimum activity durations may end at 20:34:57. There is
        a remaining time to complete the full diary (23:59:59 - 20:34:57).
        Adjust activity times probabilistically according to the maximum activity duration
        and this remaining time, making the diary end at exactly 23:59:59.

        Args:
            activities (list[Row]): list of generated activities (stays and moves) for
                the agent for all of the specified dates. Each activity is a spark
                row object.
            user_id (int): agent identifier.
            agent_stay_type_sequence (list[str]): list of generated stay types,
                each represented by a string indicating the stay type.
            home_location (tuple[float,float]): coordinates of home location.
            work_location (tuple[float,float]): coordinates of work location.
            date (datetime.date): date for activity sequence generation, used for
                timestamps and random seed generation.
            start_of_date (datetime.datetime): timestamp of current date at 00:00:00.
            end_of_date (datetime.datetime): timestamp of current date at 23:59:59.
        """
        date_activities = self.create_agent_activities_min_duration(
            user_id, agent_stay_type_sequence, home_location, work_location, date, start_of_date, end_of_date
        )
        remaining_time = (end_of_date - date_activities[-1].final_timestamp).total_seconds() / 3600.0

        if remaining_time != 0:
            self.adjust_activity_times(
                date_activities,
                remaining_time,
                user_id,
                date,
                start_of_date,
                end_of_date,
            )
        activities += date_activities

    def add_date_activities(self, date: datetime.date, activities: list[Row]):
        """
        Generate activity (stays and moves) rows for a specific date according to
        parameters.

        Args:
            date (datetime.date): date for activity sequence generation, used for
                timestamps and random seed generation.
            activities (list[Row]): list of generated activities (stays and moves) for
                the agent for all of the specified dates. Each activity is a spark
                row object.
        """
        # Start of date, end of date: datetime object generation
        start_of_date = datetime.datetime.combine(date, datetime.time(0, 0, 0))
        end_of_date = datetime.datetime.combine(date, datetime.time(23, 59, 59))
        for user_id in range(self.number_of_users):
            # generate user information:
            agent_stay_type_sequence = self.generate_stay_type_sequence(user_id, date)
            home_location = self.generate_home_location(user_id)
            work_location = self.generate_work_location(user_id, home_location)
            self.add_agent_date_activities(
                activities,
                user_id,
                agent_stay_type_sequence,
                home_location,
                work_location,
                date,
                start_of_date,
                end_of_date,
            )

    def generate_activities(self) -> list[Row]:
        """
        Generate activity and trip rows according to parameters.

        Returns:
            list[Row]: list of generated activities and trips for the agent for all
                of the specified dates. Each activity/trip is a spark row object.
        """
        activities = []
        for date in self.date_range:
            self.add_date_activities(date, activities)
        return activities

    def generate_lonlat_at_distance(self, lon1: float, lat1: float, d: float, seed: int) -> tuple[float, float]:
        """
        Given a point (lon, lat) and a distance, in meters, calculate a new random
        point that is exactly at the specified distance of the provided lon, lat.

        Args:
            lon1 (float): longitude of point, specified in decimal degrees.
            lat1 (float): latitude of point, specified in decimal degrees.
            d (float): distance, in meters.
            seed (int): random seed integer.

        Returns:
            tuple[float, float]: coordinates of randomly generated point.
        """
        r = 6_371_000  # Radius of earth in meters.

        d_x = Random(seed).uniform(0, d)
        d_y = sqrt(d**2 - d_x**2)

        # firstly, convert lat to radians for later
        lat1_radians = lat1 * pi / 180.0

        # how many meters correspond to one degree of latitude?
        deg_to_meters = r * pi / 180  # aprox. 111111 meters
        # thus, the northwards displacement, in degrees of latitude is:
        north_delta = d_y / deg_to_meters

        # but one degree of longitude does not always correspond to the
        # same distance... depends on the latitude at where you are!
        parallel_radius = abs(r * cos(lat1_radians))
        deg_to_meters = parallel_radius * pi / 180  # variable
        # thus, the eastwards displacement, in degrees of longitude is:
        east_delta = d_x / deg_to_meters

        final_lon = lon1 + east_delta * Random(seed).choice([-1, 1])
        final_lat = lat1 + north_delta * Random(seed).choice([-1, 1])

        return (final_lon, final_lat)

    def generate_home_location(self, agent_id: int) -> tuple[float, float]:
        """
        Generate random home location based on bounding box limits.

        Args:
            agent_id (int): identifier of agent, used for random seed generation.

        Returns:
            tuple[float,float]: coordinates of generated home location.
        """
        seed_lon = self.random_seed_number_generator(1, agent_id)
        seed_lat = self.random_seed_number_generator(2, agent_id)
        hlon = Random(seed_lon).uniform(self.longitude_min, self.longitude_max)
        hlat = Random(seed_lat).uniform(self.latitude_min, self.latitude_max)
        return (hlon, hlat)

    def generate_work_location(
        self, agent_id: int, home_location: tuple[float, float], seed: int = 4
    ) -> tuple[float, float]:
        """
        Generate random work location based on home location and maximum distance to
        home. If the work location falls outside of bounding box limits, try again.

        Args:
            agent_id (int): identifier of agent, used for random seed generation.
            home_location (tuple[float,float]): coordinates of home location.
            seed (int, optional): random seed integer. Defaults to 4.

        Returns:
            tuple[float,float]: coordinates of generated work location.
        """
        seed_distance = self.random_seed_number_generator(seed - 1, agent_id)
        random_distance = Random(seed_distance).uniform(self.home_work_distance_min, self.home_work_distance_max)
        hlon, hlat = home_location
        seed_coords = self.random_seed_number_generator(seed, agent_id)
        wlon, wlat = self.generate_lonlat_at_distance(hlon, hlat, random_distance, seed_coords)

        if not (self.longitude_min < wlon < self.longitude_max) or not (
            self.latitude_min < wlat < self.latitude_max
        ):  # outside limits
            seed += 1
            wlon, wlat = self.generate_work_location(agent_id, home_location, seed=seed)

        return (wlon, wlat)

    def generate_other_location(
        self,
        agent_id: int,
        date: datetime.date,
        activity_number: int,
        home_location: tuple[float, float],
        previous_location: tuple[float, float],
        seed: int = 6,
    ) -> tuple[float, float]:
        """
        Generate other activity location based on previous location and maximum distance
        to previous location. If there is no previous location (this is the first
        activity of the day), then the home location is considered as previous location.
        If the location falls outside of bounding box limits, try again.

        Args:
            agent_id (int): identifier of agent, used for random seed generation.
            date (datetime.date): date, used for random seed generation.
            activity_number (int): act position, used for random seed generation.
            home_location (tuple[float,float]): coordinates of home location.
            previous_location (tuple[float,float]): coordinates of previous location.
            seed (int, optional): random seed integer. Defaults to 6.

        Returns:
            tuple[float,float]: coordinates of generated location.
        """
        seed_distance = self.random_seed_number_generator(seed - 1, agent_id)
        random_distance = Random(seed_distance).uniform(self.other_distance_min, self.other_distance_max)
        if previous_location is None:
            plon, plat = home_location
        else:
            plon, plat = previous_location

        seed_coords = self.random_seed_number_generator(seed, agent_id)
        olon, olat = self.generate_lonlat_at_distance(plon, plat, random_distance, seed_coords)
        if not (self.longitude_min < olon < self.longitude_max) or not (
            self.latitude_min < olat < self.latitude_max
        ):  # outside limits
            seed += 1
            olon, olat = self.generate_other_location(
                agent_id, date, activity_number, home_location, previous_location, seed=seed
            )

        return (olon, olat)

    def generate_stay_duration(
        self, agent_id: int, date: datetime.date, i: int, stay_type: str, remaining_time: float
    ) -> float:
        """
        Generate stay duration probabilistically based on activity type
        abd remaining time.

        Args:
            agent_id (int): identifier of agent, used for random seed generation.
            date (datetime.date): date, used for random seed generation.
            i (int): activity position, used for random seed generation.
            stay_type (str): type of stay. Shall be "home", "work" or "other".
            remaining_time (float): same units as durations.

        Returns:
            float: generated activity duration.
        """
        if stay_type == "home":
            min_duration = self.home_duration_min
            max_duration = self.home_duration_max
        elif stay_type == "work":
            min_duration = self.work_duration_min
            max_duration = self.work_duration_max
        elif stay_type == "other":
            min_duration = self.other_duration_min
            max_duration = self.other_duration_max
        else:
            raise ValueError
        seed = self.random_seed_number_generator(7, agent_id, date, i)
        max_value = min(max_duration, min_duration + remaining_time)
        return Random(seed).uniform(min_duration, max_value)

    def generate_min_stay_duration(self, stay_type: str) -> float:
        """
        Generate minimum stay duration based on stay type specifications.

        Args:
            stay_type (str): type of stay. Shall be "home", "work" or "other".

        Returns:
            float: minimum stay duration.
        """
        if stay_type == "home":
            return self.home_duration_min
        elif stay_type == "work":
            return self.work_duration_min
        elif stay_type == "other":
            return self.other_duration_min
        else:
            raise ValueError

    def remove_consecutive_stay_types(self, stay_sequence_list: list[str], stay_types_to_group: set[str]) -> list[str]:
        """
        Generate new list replacing consecutive stays of the same type by
        a unique stay as long as the stay type is contained in the
        "stay_types_to_group" list.

        Args:
            stay_sequence_list (list[str]): input stay type list.
            stay_types_to_group (set[str]): stay types to group.

        Returns:
            list[str]: output stay sequence list.
        """
        new_stay_sequence_list = []
        previous_stay = None
        for stay in stay_sequence_list:
            if stay == previous_stay and stay in stay_types_to_group:
                pass
            else:
                new_stay_sequence_list.append(stay)
            previous_stay = stay
        return new_stay_sequence_list

    def generate_stay_type_sequence(self, agent_id: int, date: datetime.date) -> list[str]:
        """
        Generate the sequence of stay types for an agent for a specific date
        probabilistically based on the superset sequence and specified
        probabilities.
        Replace 'home'-'home' and 'work'-'work' sequences by just 'home' or
        'work'.

        Args:
            agent_id (int): identifier of agent, used for random seed generation.
            date (datetime.date): date for activity sequence generation, used for
                random seed generation.

        Returns:
            list[str]: list of generated stay types, each represented by a string
                indicating the stay type (e.g. "home", "work", "other").
        """
        stay_type_sequence = []
        for i, stay_type in enumerate(self.stay_sequence_superset):
            stay_weight = self.stay_sequence_probabilities[i]
            seed = self.random_seed_number_generator(0, agent_id, date, i)
            if Random(seed).random() < stay_weight:
                stay_type_sequence.append(stay_type)
        stay_type_sequence = self.remove_consecutive_stay_types(stay_type_sequence, {"home", "work"})
        return stay_type_sequence
