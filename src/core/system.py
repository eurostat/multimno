from core.configuration import ConfigurationProvider
from core.log import LoggerProvider
from core.spark_session import SparkSessionProvider
from pyspark import SparkFiles


def init_singletons(general_config_path: str, component_config_path: str = ''):
    root_path = SparkFiles.getRootDirectory()
    
    # Init configuration
    config_provider = ConfigurationProvider(general_config_path, component_config_path)
    # Init Spark Session
    session_provider = SparkSessionProvider(config_provider.config)
    # Init logger
    LoggerProvider(session_provider.spark, config_provider.config)