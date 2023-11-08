import pyspark.sql.functions as psf
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType

class SESSION_TYPES:
    LOCAL_SESSION = "local"
    STANDALONE_SESSION = "standalone"
    YARN_SESSION = "yarn"

def build_local_session():
    return SparkSession.builder\
        .config("spark.driver.host", "localhost")\
        .config("spark.eventLog.enabled", "true")\
        .config("spark.eventLog.dir", "/tmp")\
        .config("spark.history.fs.logDirectory", "/tmp")\
            .appName("Test-multimno-submit").getOrCreate()

build_session = {SESSION_TYPES.LOCAL_SESSION: build_local_session}

if __name__=="__main__":
    SESSION = SESSION_TYPES.LOCAL_SESSION

    spark = build_session[SESSION]()

    sc = spark.sparkContext.getOrCreate()

    input_path = "/opt/data/input/od_pairs.txt"
    output_path = "/opt/data/output/od_pairs.parquet"


    schema = StructType() \
        .add("origin",StringType(),True) \
        .add("destination",StringType(),True)
        
    df = spark.read.option("delimiter", "|").option("header", False).schema(schema).csv(input_path)

    df = df.withColumn('test', psf.lit('patata'))

    df.write.mode('overwrite').parquet(output_path)
    df = spark.read.parquet(output_path)
    df.show()
