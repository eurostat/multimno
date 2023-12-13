
from configparser import ConfigParser
try:
    from sedona.spark import SedonaContext
except ImportError as importerror:
    print("Unable to import Sedona")

from pyspark.sql import SparkSession

SPARK_CONFIG_KEY = "Spark"


def generate_spark_session(config: ConfigParser):
    """Function that generates a Spark Sedona session.

    Args:
        config (ConfigParser): Object with the final configuration.

    Returns:
        SparkSession: Session of spark.
    """
    conf_dict = dict(config[SPARK_CONFIG_KEY])

    if "sedona_enabled" in conf_dict.keys():
        sedona_enabled = eval(conf_dict.pop("sedona_enabled"))
    else:
        sedona_enabled = True
    
    if sedona_enabled == False:
        master = conf_dict.pop('spark.master')
        session_name = conf_dict.pop('session_name')

        # Generic Spark session
        spark = SparkSession.builder.appName(session_name).master(master)

        for key, value in conf_dict.items():
            spark = spark.config(key, value)

        session = spark.getOrCreate()
        session.conf.set("spark.sql.session.timeZone", "UTC")
        
        return session 

    else:
        builder = SedonaContext.builder().appName(
            f'{session_name}'
        ).master(
            master
        )

        # Configuration file spark configs
        for k, v in conf_dict.items():
            builder = builder.config(k, v)
        
        sc = spark.sparkContext
        sc.setSystemProperty("sedona.global.charset", "utf8")

        # Set log
        sc.setLogLevel('ERROR')
        log4j = sc._jvm.org.apache.log4j
        log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

        return spark
