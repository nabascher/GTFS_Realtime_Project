#!/bin/bash

PYTHON_SCRIPT='/home/nate/Documents/Python_Projects/GTFS_Analysis/Spark_Streaming_Session_Creator_v1.py'

spark-submit \
    --master spark://thinkpad-t480:7077 \
    --deploy-mode client \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
    --conf spark.executor.memory=4g \
    --conf spark.driver.memory=2g \
    --conf spark.streaming.kafka.consumer.cache.enabled=false \
    $PYTHON_SCRIPT
