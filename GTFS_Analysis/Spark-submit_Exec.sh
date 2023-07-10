#!/bin/bash

PYTHON_SCRIPT='/home/nate/Documents/Python_Projects/GTFS_Analysis/Spark_Streaming_Session_Creator_v1.py'
SEDONA_JARS='/home/nate/.ivy2/jars/org.apache.sedona_sedona-spark-shaded-3.4_2.12-1.4.1.jar'

spark-submit \
    --master spark://thinkpad-t480:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --jars  $SEDONA_JARS\
    --conf spark.executor.memory=4g \
    --conf spark.driver.memory=2g \
    --conf spark.streaming.kafka.consumer.cache.enabled=false \
    $PYTHON_SCRIPT
