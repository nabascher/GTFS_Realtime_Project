from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from sedona.utils.adapter import Adapter

#Define Spark streaming conetext and build app
spark = SparkSession.builder.appName("GTFS-realtime Ingestion").getOrCreate()

test_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "mbta-realtime") \
  .load()
test_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 

#Import data into sedona spatial df
spatial_df = Adapter.toDf(test_df, spark)

#Print result
spatial_df.show()

#Start the streaming query to process data in real-time
query = spatial_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

