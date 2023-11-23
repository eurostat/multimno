import pyspark.sql.functions as psf
from sedona.spark import SedonaContext
from sedona.spark import SedonaPyDeck
from pyspark.sql.types import StructType, StructField, StringType
from sedona.sql.types import GeometryType
import sys

# Functions
def export_map(df, output_path):
    # PyDeck
    fill_color=[255, 12, 250]
    census_map = SedonaPyDeck.create_choropleth_map(df=df, fill_color=fill_color)
    census_map.to_html(output_path)

def build_local_session():
    builder = SedonaContext.builder().appName(
        'Sedona Session'
    )
    # Set sedona session
    spark = SedonaContext.create(builder.getOrCreate())
    sc = spark.sparkContext
    sc.setSystemProperty("sedona.global.charset", "utf8")
    return spark, sc


if __name__=="__main__":

    print("[*] START")

    spark, sc = build_local_session()


    census_input_path = "/opt/data/input/euskadi.parquet"
    census_output_path = "/opt/data/output/census/"
    census_vis_path = "/opt/data/output/census.html"


    schema = StructType([
        StructField("geometry", GeometryType(), nullable=False),
        StructField("NPRO", StringType(), nullable=True),
    ])
        
    df = spark.read.schema(schema).format('geoparquet').load(census_input_path)

    df.withColumn('NPRO', psf.trim('NPRO')).withColumn('NCA', psf.lit('Euskadi')).createOrReplaceTempView("census")

    dissolved_census = spark.sql(f"""
        SELECT NCA, ST_Union_Aggr(geometry) AS geometry 
        FROM census
        GROUP BY NCA
    """)

    dissolved_census.write.format(
            'geoparquet',  # File format
        ).mode("overwrite").save(census_output_path)

    df = spark.read.format('geoparquet').load(census_output_path)
    export_map(df, census_vis_path)
    print("[*] FINISHED")

    spark.stop()
    sys.exit(0)
