from datetime import datetime, timedelta
from pyspark.sql.types import Row
import calendar as cal
from multimno.core.constants.reserved_dataset_ids import ReservedDatasetIDs
from multimno.core.constants.error_types import UeGridIdType


def generate_input_lps_data(start_date: str, end_date: str) -> list[Row]:
    """
    Generate input population grid data.
    """
    # create end date from start date. Just date not time
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)

    # ─────────────────────────────────────────────────────────────────────────────
    # Partition 1: day_type="all", time_interval="all"
    # ─────────────────────────────────────────────────────────────────────────────
    # Device 1:
    # - 2 tiles at >=70% of device observation => labeled UE
    # - 1 tile at <70% of device observation, but will be labeled UE in night_time
    # - 2 tiles with very low values => discarded
    # - 1 device row (id_type="device") storing overall metrics
    # - 1 outbound tile with low lps but high frequency
    # Device 2:
    # - discarded as rarely observed
    # - 1 device row (id_type="device") with low lps
    # - 1 tile with low lps
    # Device row: (LPS=100, used for ratio reference)
    all_intervals = [
        Row(
            user_id=b"device1",
            grid_id=UeGridIdType.DEVICE_OBSERVATION_GRID_ID,
            lps=100,
            total_frequency=100,
            frequency_mean=10.0,
            frequency_std=5.0,
            regularity_mean=8.0,
            regularity_std=2.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.DEVICE_OBSERVATION_STR,
            user_id_modulo=0,
        ),
        Row(
            user_id=b"device2",
            grid_id=UeGridIdType.DEVICE_OBSERVATION_GRID_ID,
            lps=1,
            total_frequency=1,
            frequency_mean=10.0,
            frequency_std=5.0,
            regularity_mean=8.0,
            regularity_std=2.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.DEVICE_OBSERVATION_STR,
            user_id_modulo=0,
        ),
        # Two UE tiles based on LPS (≥70% threshold vs. device's LPS=100 → each tile LPS=75 → ratio=0.75)
        Row(
            user_id=b"device1",
            grid_id=1,
            lps=75,
            total_frequency=75,
            frequency_mean=5.0,
            frequency_std=1.0,
            regularity_mean=4.0,
            regularity_std=1.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        # this tile will be labeled as home as it has high LPS in all:night_time
        Row(
            user_id=b"device1",
            grid_id=2,
            lps=75,
            total_frequency=75,
            frequency_mean=5.0,
            frequency_std=1.0,
            regularity_mean=4.0,
            regularity_std=1.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        # 1 UE tile with low lps in all:all, will be high in work_days:working_hours
        # will be labeled as work
        Row(
            user_id=b"device1",
            grid_id=3,
            lps=65,
            total_frequency=40,
            frequency_mean=5.0,
            frequency_std=1.0,
            regularity_mean=4.0,
            regularity_std=1.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        # 1 tile with low lps for UE
        Row(
            user_id=b"device1",
            grid_id=4,
            lps=60,
            total_frequency=50,
            frequency_mean=5.0,
            frequency_std=1.0,
            regularity_mean=4.0,
            regularity_std=1.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        # 1 outbound tile with low lps but high frequency
        Row(
            user_id=b"device1",
            grid_id=222,
            lps=60,
            total_frequency=80,
            frequency_mean=5.0,
            frequency_std=1.0,
            regularity_mean=4.0,
            regularity_std=1.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.ABROAD_STR,
            user_id_modulo=0,
        ),
        # Two discarded with gap tiles (≤20% threshold vs. max LPS=75 → each tile LPS=10 → gap = 50 > 15)
        Row(
            user_id=b"device1",
            grid_id=5,
            lps=10,
            total_frequency=10,
            frequency_mean=1.0,
            frequency_std=0.5,
            regularity_mean=0.8,
            regularity_std=0.2,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        Row(
            user_id=b"device1",
            grid_id=6,
            lps=10,
            total_frequency=10,
            frequency_mean=1.0,
            frequency_std=0.5,
            regularity_mean=0.8,
            regularity_std=0.2,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
        # 1 discarded tile for device 2
        Row(
            user_id=b"device2",
            grid_id=1,
            lps=1,
            total_frequency=1,
            frequency_mean=1.0,
            frequency_std=0.0,
            regularity_mean=0.0,
            regularity_std=0.0,
            season="all",
            start_date=start_date,
            end_date=end_date,
            day_type="all",
            time_interval="all",
            id_type=UeGridIdType.GRID_STR,
            user_id_modulo=0,
        ),
    ]

    # ─────────────────────────────────────────────────────────────────────────────
    # Partition 2: day_type="all", time_interval="night_time"
    # ─────────────────────────────────────────────────────────────────────────────
    # - 1 tile with device observation for this interval
    # - 2 tiles to be labeled as UE
    # - 1 tile among these to be labeled as home: high LPS>90% of device observation

    all_intervals.extend(
        [
            # Device row: (LPS=100, used for ratio reference)
            Row(
                user_id=b"device1",
                grid_id=UeGridIdType.DEVICE_OBSERVATION_GRID_ID,
                lps=100,
                total_frequency=100,
                frequency_mean=10.0,
                frequency_std=5.0,
                regularity_mean=8.0,
                regularity_std=2.0,
                season="all",
                start_date=start_date,
                end_date=end_date,
                day_type="all",
                time_interval="night_time",
                id_type=UeGridIdType.DEVICE_OBSERVATION_STR,
                user_id_modulo=0,
            ),
            # this tile will be labeled as home as it has high LPS in all:night_time
            Row(
                user_id=b"device1",
                grid_id=2,
                lps=95,
                total_frequency=95,
                frequency_mean=5.0,
                frequency_std=1.0,
                regularity_mean=4.0,
                regularity_std=1.0,
                season="all",
                start_date=start_date,
                end_date=end_date,
                day_type="all",
                time_interval="night_time",
                id_type=UeGridIdType.GRID_STR,
                user_id_modulo=0,
            ),
        ]
    )

    # ─────────────────────────────────────────────────────────────────────────────
    # Partition 3: day_type="work_day", time_interval="working_hours"
    # ─────────────────────────────────────────────────────────────────────────────
    # - 1 tile with device observation for this interval
    # - 1 tile to be labeled as UE and work

    all_intervals.extend(
        [
            # Device row: (LPS=100, used for ratio reference)
            Row(
                user_id=b"device1",
                grid_id=UeGridIdType.DEVICE_OBSERVATION_GRID_ID,
                lps=100,
                total_frequency=100,
                frequency_mean=10.0,
                frequency_std=5.0,
                regularity_mean=8.0,
                regularity_std=2.0,
                season="all",
                start_date=start_date,
                end_date=end_date,
                day_type="workdays",
                time_interval="working_hours",
                id_type=UeGridIdType.DEVICE_OBSERVATION_STR,
                user_id_modulo=0,
            ),
            # 1 UE tile with low lps in all:all, will be high in work_days:working_hours
            # will be labeled as work
            Row(
                user_id=b"device1",
                grid_id=3,
                lps=95,
                total_frequency=40,
                frequency_mean=5.0,
                frequency_std=1.0,
                regularity_mean=4.0,
                regularity_std=1.0,
                season="all",
                start_date=start_date,
                end_date=end_date,
                day_type="workdays",
                time_interval="working_hours",
                id_type=UeGridIdType.GRID_STR,
                user_id_modulo=0,
            ),
        ]
    )

    return all_intervals


def generate_expected_ue_labels(start_date: str, end_date: str) -> list[Row]:
    """
    Generate expected output data by aggregating population over 1km grid.
    """
    # create end date from start date. Just date not time
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)

    expected_labels = [
        # 4 tiles labeled as UE
        Row(
            user_id=b"device1",
            grid_id=1,
            label="ue",
            label_rule="ue_1",
            id_type=UeGridIdType.GRID_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
        Row(
            user_id=b"device1",
            grid_id=2,
            label="ue",
            label_rule="ue_1",
            id_type=UeGridIdType.GRID_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
        Row(
            user_id=b"device1",
            grid_id=3,
            label="ue",
            label_rule="ue_2",
            id_type=UeGridIdType.GRID_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
        Row(
            user_id=b"device1",
            grid_id=222,
            label="ue",
            label_rule="ue_3",
            id_type=UeGridIdType.ABROAD_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
        # 1 tile labeled as home
        Row(
            user_id=b"device1",
            grid_id=2,
            label="home",
            label_rule="h_2",
            id_type=UeGridIdType.GRID_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
        # 1 tile labeled as work
        Row(
            user_id=b"device1",
            grid_id=3,
            label="work",
            label_rule="w_1",
            id_type=UeGridIdType.GRID_STR,
            start_date=start_date,
            end_date=end_date,
            season="all",
            user_id_modulo=0,
        ),
    ]

    return expected_labels


def generate_expected_qm(start_date: str, end_date: str) -> list[Row]:
    """
    Generate expected output data by aggregating weighted_device_count over 1km grid and labels.
    """
    # Parse the start and end dates
    start_date = datetime.strptime(start_date, "%Y-%m")
    end_date = datetime.strptime(end_date, "%Y-%m")
    end_date = end_date + timedelta(days=cal.monthrange(end_date.year, end_date.month)[1] - 1)
    expected_labeling_quality_data = [
        Row(
            metric="ue_1",
            count=2,
            min=2,
            max=2,
            avg=2,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="ue_2",
            count=1,
            min=1,
            max=1,
            avg=1,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="ue_3",
            count=1,
            min=1,
            max=1,
            avg=1,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="h_2",
            count=1,
            min=1,
            max=1,
            avg=1,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="w_1",
            count=1,
            min=1,
            max=1,
            avg=1,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="loc_na",
            count=5,
            min=5,
            max=5,
            avg=5,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="ue_na",
            count=3,
            min=3,
            max=3,
            avg=3,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="device_filter_1",
            count=1,
            min=0,
            max=0,
            avg=0,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="device_filter_2",
            count=0,
            min=0,
            max=0,
            avg=0,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
        Row(
            metric="ue_abroad",
            count=1,
            min=0,
            max=0,
            avg=0,
            start_date=start_date,
            end_date=end_date,
            season="all",
        ),
    ]

    return expected_labeling_quality_data
