from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sedona.spark import *

# Configure sedona
config = SedonaContext.builder(). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.4_2.12:1.4.1,'
           'org.datasyslab:geotools-wrapper:1.4.0-28.2'). \
    getOrCreate()
sedona = SedonaContext.create(config)

#Define Spark streaming context and build app
spark = SparkSession \
  .builder \
  .appName("Spark_Streaming_Session_Creator_v1.py") \
  .getOrCreate()

# Load kafka data into spark streaming session
stream_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mbta-realtime") \
  .load()
stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
stream_df.printSchema()

#Parse out JSON and extract geometry field
parsed_df = stream_df \
  .select(col("value") \
  .cast("string").alias("json_value")) \
  .selectExpr("get_json_object(json_value, '$.vehicle.position') AS geometry")
parsed_df.printSchema()
parsed_df.createOrReplaceTempView("parsed_json")

#Convert the geometry to wkt and create spatial dataframe
spatial_df = sedona.sql("SELECT st_geomFromWKT(geometry) AS wkt_geometry FROM parsed_json")
spatial_df.printSchema()

#Start the streaming query to process data in realtime
query = spatial_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \

query.awaitTermination()

spark.stop()