"""

"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
)
from multimno.core.data_objects.data_object import ParquetDataObject
from multimno.core.constants.columns import ColNames


class BronzeInboundEstimationFactorsDataObject(ParquetDataObject):
    """
    Class that contains the deduplication and mno-to-target-population factors for usage on the estimation
    of inbound tourism data.
    """

    ID = "BronzeInboundEstimationFactorDO"
    SCHEMA = StructType(
        [
            StructField(ColNames.iso2, StringType(), nullable=False),
            StructField(ColNames.deduplication_factor, FloatType(), nullable=True),
            StructField(ColNames.mno_to_target_population_factor, FloatType(), nullable=True),
        ]
    )

    PARTITION_COLUMNS = []
