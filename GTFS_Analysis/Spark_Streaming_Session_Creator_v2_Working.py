from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sedona.spark import *
from shapely import wkt
import geopandas as gpd
import pandas as pd

#Add wkt csvs as geopandas geodataframes
chinatown_df = pd.read_csv(r'/home/nate/Documents/Python_Projects/GTFS_Analysis/Geofences/chinatown_wkt.csv')
north_end_df = pd.read_csv(r'/home/nate/Documents/Python_Projects/GTFS_Analysis/Geofences/Northend_wkt.csv')

chinatown_df['geometry'] = chinatown_df['WKT'].apply(wkt.loads)
north_end_df['geometry'] = north_end_df['WKT'].apply(wkt.loads)

chinatown_gdf = gpd.GeoDataFrame(chinatown_df, crs='EPSG:3857')
north_end_gdf = gpd.GeoDataFrame(north_end_df, crs='EPSG:3857')

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
spark.sparkContext.setLogLevel("WARN")

# Load kafka data into spark streaming session
stream_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mbta-realtime") \
  .load()
stream_df.printSchema()

#Cast json value as string
stream_df = stream_df.withColumn("value", stream_df["value"].cast("STRING"))

#Define JSON schema for deserialization
json_schema = StructType([
    StructField("vehicle", StructType([
        StructField("id", StringType()),
        StructField("label", StringType())
    ])),
    StructField("position", StructType([
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", IntegerType())
    ])),
    StructField("current_stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("current_status", StringType()),
    StructField("timestamp", LongType()),
    StructField("occupancy_status", StringType(), True),  # Nullable
    StructField("occupancy_percentage", IntegerType(), True)  # Nullable
])

#Deserialize the JSON 
stream_df = stream_df.withColumn("value", from_json("value", json_schema))
stream_df.printSchema()

#Grab specific fields from schema
stream_df = stream_df.select("value.vehicle", "value.position", "value.timestamp")

#Parse out lat and long information
latitude_expr = concat(lit(""), col("position.latitude")).alias("latitude")
longitude_expr = concat(lit(""), col("position.longitude")).alias("longitude")

# Create new columns with lat and long information
parsed_df = stream_df.withColumn("latitude", latitude_expr).withColumn("longitude", longitude_expr)
parsed_df.printSchema()
parsed_df.createOrReplaceTempView("parsed_json")

#Convert the geometry to wkt and create spatial dataframe
spatial_df = sedona.sql("SELECT timestamp, st_point(longitude, latitude) AS geometry FROM parsed_json")

#Start the streaming query to process data in realtime
query = spatial_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \

query.awaitTermination()

spark.stop()