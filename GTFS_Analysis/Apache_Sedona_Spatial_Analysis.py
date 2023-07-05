from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from sedona.register import SedonaRegistrator
from sedona.utils.adapter import Adapter

#Initialize Spark and Sedona
spark = SparkSession.builder \
    .appName("GTFS-realtime Ingestion") \
    .getOrCreate()

SedonaRegistrator.registerAll(spark)

#Define Streaming Context
ssc = StreamingContext(spark.sparkContext, batchDurationSeconds=30)

# Start the computation
ssc.start()             
ssc.awaitTermination()