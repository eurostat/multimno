import pytest
from configparser import ConfigParser
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    Row,
    StringType,
    StructField,
    StructType,
    IntegerType,
    TimestampType,
    FloatType,
    ShortType,
    ByteType,
)
import pyspark.sql.functions as F

from multimno.core.constants.columns import ColNames
from multimno.core.constants.error_types import SemanticErrorType
from multimno.core.data_objects.silver.silver_semantic_quality_metrics import (
    SilverEventSemanticQualityMetrics,
)
from multimno.core.data_objects.silver.silver_semantic_quality_warnings_log_table import (
    SilverEventSemanticQualityWarningsLogTable,
)
from tests.test_code.fixtures import spark_session as spark

fixtures = [spark]


@pytest.fixture
def expected_warnings(spark):

    warnings = [
        Row(
            **{
                ColNames.date: datetime.date(2024, 1, 5),
                "Error 1": 4.081632614135742,
                "Error 2": 8.163265228271484,
                "Error 3": 12.244897842407227,
                "Error 4": 16.32653045654297,
                "Error 5": 20.40816307067871,
                "Error 1 upper control limit": 4.140876293182373,
                "Error 2 upper control limit": 8.105657577514648,
                "Error 3 upper control limit": 12.070438385009766,
                "Error 4 upper control limit": 16.035219192504883,
                "Error 5 upper control limit": 20.0,
                "Error 1 display warning": False,
                "Error 2 display warning": True,
                "Error 3 display warning": True,
                "Error 4 display warning": True,
                "Error 5 display warning": True,
                "execution_id": datetime.datetime(2024, 1, 6, 14),
                ColNames.year: 2024,
                ColNames.month: 1,
                ColNames.day: 5,
            }
        )
    ]

    warnings_df = spark.createDataFrame(warnings, schema=SilverEventSemanticQualityWarningsLogTable.SCHEMA)

    return warnings_df


def set_input_data(spark: SparkSession, config: ConfigParser):
    metrics_test_data_path = config["Paths.Silver"]["event_device_semantic_quality_metrics"]

    timestamp = datetime.datetime(year=2024, month=2, day=1, hour=12)
    variable = ColNames.cell_id
    metrics = []

    def get_value(error_code, day):
        if day < 5:
            if error_code == SemanticErrorType.NO_ERROR:
                return 1000
            elif error_code == SemanticErrorType.CELL_ID_NON_EXISTENT:
                return 100 + day
            elif error_code == SemanticErrorType.CELL_ID_NOT_VALID:
                return 200 + day
            elif error_code == SemanticErrorType.INCORRECT_EVENT_LOCATION:
                return 300 + day
            elif error_code == SemanticErrorType.SUSPICIOUS_EVENT_LOCATION:
                return 400 + day
            elif error_code == SemanticErrorType.DIFFERENT_LOCATION_DUPLICATE:
                return 500 + day
        else:
            if error_code == SemanticErrorType.NO_ERROR:
                return 950
            elif error_code == SemanticErrorType.CELL_ID_NON_EXISTENT:
                return 100
            elif error_code == SemanticErrorType.CELL_ID_NOT_VALID:
                return 200
            elif error_code == SemanticErrorType.INCORRECT_EVENT_LOCATION:
                return 300
            elif error_code == SemanticErrorType.SUSPICIOUS_EVENT_LOCATION:
                return 400
            elif error_code == SemanticErrorType.DIFFERENT_LOCATION_DUPLICATE:
                return 500

    error_codes = [
        SemanticErrorType.NO_ERROR,
        SemanticErrorType.CELL_ID_NON_EXISTENT,
        SemanticErrorType.CELL_ID_NOT_VALID,
        SemanticErrorType.INCORRECT_EVENT_LOCATION,
        SemanticErrorType.SUSPICIOUS_EVENT_LOCATION,
        SemanticErrorType.DIFFERENT_LOCATION_DUPLICATE,
    ]
    for day in range(1, 6):
        for error_code in error_codes:
            metrics.append(
                Row(
                    **{
                        ColNames.result_timestamp: timestamp,
                        ColNames.variable: variable,
                        ColNames.type_of_error: error_code,
                        ColNames.value: get_value(error_code, day),
                        ColNames.year: 2024,
                        ColNames.month: 1,
                        ColNames.day: day,
                    }
                )
            )

    metrics_df = spark.createDataFrame(metrics, schema=SilverEventSemanticQualityMetrics.SCHEMA)

    metrics_df.show()

    metrics_data = SilverEventSemanticQualityMetrics(
        spark,
        metrics_test_data_path,
        partition_columns=[ColNames.year, ColNames.month, ColNames.day],
    )
    metrics_data.df = metrics_df
    metrics_data.write()
