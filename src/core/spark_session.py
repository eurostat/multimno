
from configparser import ConfigParser
from sedona.spark import SedonaContext

SPARK_CONFIG_KEY = "Spark"


def generate_spark_session(config: ConfigParser):
    """Function that generates a Spark Sedona session.

    Args:
        config (ConfigParser): Object with the final configuration.

    Returns:
        SparkSession: Session of spark.
    """
    conf_dict = dict(config[SPARK_CONFIG_KEY])
    master = conf_dict.pop('spark.master')
    session_name = conf_dict.pop('session_name')

    builder = SedonaContext.builder().appName(
        f'{session_name}'
    ).master(
        master
    )

    # Configuration file spark configs
    for k, v in conf_dict.items():
        builder = builder.config(k, v)

    # Set sedona session
    spark = SedonaContext.create(builder.getOrCreate())
    sc = spark.sparkContext
    sc.setSystemProperty("sedona.global.charset", "utf8")

    return spark
