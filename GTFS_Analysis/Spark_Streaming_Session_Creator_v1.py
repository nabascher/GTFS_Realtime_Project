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
  .appName("GTFS-realtime Ingestion") \
  .getOrCreate()

# Load kafka data into spark streaming session
test_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mbta-realtime") \
  .load()
test_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 

#Parse out JSON and extract geometry field
parsed_df = test_df.select(col("value").cast("string").alias("json_value")) \
  .selectExpr("get_json_object(json_value, '$.vehicle.position') AS geometry")

#Convert the geometry to wkt format
wkt_df = parsed_df.withColumn("wkt_geometry", st_geomFromWKT(col("geometry")))

#Create the Sedona dataframe
spatial_df = wkt_df.select("wkt_geometry")

#Start the streaming query to process data in real-time
query = spatial_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

