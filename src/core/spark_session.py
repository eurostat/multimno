from configparser import ConfigParser
from sedona.spark import SedonaContext
from pyspark.sql import SparkSession
import py4j

SPARK_CONFIG_KEY = "Spark"


def generate_spark_session(config: ConfigParser):
    """Function that generates a Spark Sedona session.

    Args:
        config (ConfigParser): Object with the final configuration.

    Returns:
        SparkSession: Session of spark.
    """
    conf_dict = dict(config[SPARK_CONFIG_KEY])
    master = conf_dict.pop("spark.master")
    session_name = conf_dict.pop("session_name")

    builder = SedonaContext.builder().appName(f"{session_name}").master(master)

    # Configuration file spark configs
    for k, v in conf_dict.items():
        builder = builder.config(k, v)

    # Set sedona session
    spark = SedonaContext.create(builder.getOrCreate())
    sc = spark.sparkContext
    sc.setSystemProperty("sedona.global.charset", "utf8")

    # Set log
    sc.setLogLevel("ERROR")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)

    return spark


def check_if_data_path_exists(spark: SparkSession, data_path: str):
    """
    Checks whether data path exists, returns True if it does, False if not

    Args:
        spark (SparkSession): active SparkSession
        data_path (str): path to check

    Returns:
        Bool: Whether the passed path exists
    """
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(data_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    return fs.exists(spark._jvm.org.apache.hadoop.fs.Path(data_path))


def check_or_create_data_path(spark: SparkSession, data_path: str):
    """
    Create the provided path on a file system. If path already exists, do nothing.

    Args:
        spark (SparkSession): active SparkSession
        data_path (str): path to check
    """
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(data_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(data_path)
    if not fs.exists(path):
        fs.mkdirs(path)


def delete_file_or_folder(spark: SparkSession, data_path: str):
    """
    Deletes file or folder with given path

    Args:
        spark (SparkSession): Currently active spark session
        data_path (str): Path to remove
    """
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(data_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(data_path)
    fs.delete(path, True)


def list_all_files_recursively(spark: SparkSession, data_path: str) -> list[str]:
    """
    If path is a file, returns a singleton list with this path.
    If path is a folder, return a list of all files in this folder and any of its subfolders

    Args:
        spark (SparkSession): Currently active spark session
        data_path (str): Path to list the files of

    Returns:
        list[str]: A list of all files in that folder and its subfolders
    """
    conf = spark._jsc.hadoopConfiguration()
    uri = spark._jvm.java.net.URI.create(data_path)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
    path = spark._jvm.org.apache.hadoop.fs.Path(data_path)
    return list_all_files_helper(path, fs, conf)


def list_all_files_helper(
    path: py4j.java_gateway.JavaObject, fs: py4j.java_gateway.JavaClass, conf: py4j.java_gateway.JavaObject
) -> list[str]:
    """
    This function is used by list_all_files_recursively. This should not be called elsewhere
    Recursively traverses the file tree from given spot saving all files to a list and returns it.

    Args:
        path (str): py4j.java_gateway.JavaObject: Object from parent function
        hadoop (py4j.java_gateway.JavaPackage): Object from parent function
        fs (py4j.java_gateway.JavaClass): Object from parent function
        conf (py4j.java_gateway.JavaObject): Object from parent function

    Returns:
        list: List of all files this folder and subdirectories of this folder.
    """
    files_list = []

    for f in fs.listStatus(path):
        if f.isDirectory():
            files_list.extend(list_all_files_helper(f.getPath(), fs, conf))
        else:
            files_list.append(str(f.getPath()))

    return files_list


def list_parquet_partition_col_values(spark: SparkSession, data_path: str) -> list[str]:
    """
    Lists all partition column values given a partition parquet folder

    Args:
        spark (SparkSession): Currently active spark session
        data_path (str): Path of parquet

    Returns:
        str, list[str]: Name of partition column, List of partition col values
    """

    hadoop = spark._jvm.org.apache.hadoop
    fs = hadoop.fs.FileSystem
    conf = hadoop.conf.Configuration()
    path = hadoop.fs.Path(data_path)

    partitions = []
    for f in fs.get(conf).listStatus(path):
        if f.isDirectory():
            partitions.append(str(f.getPath().getName()))

    if len(partitions) == 0:
        return None, None

    partition_col = partitions[0].split("=")[0]

    partitions = [p.split("=")[1] for p in partitions]
    return partition_col, sorted(partitions)
