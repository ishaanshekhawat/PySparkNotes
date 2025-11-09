#!/usr/bin/python

# Demo - Input Sources - Kafka
# 
# This is a Kafka Streaming word count application

# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Create Spark Session
spark = SparkSession\
.builder\
.master("local")\
.appName("Kafka Source")\
.getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

# Create Streaming DataFrame by reading data from kafka.
initDF = spark\
.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "127.0.0.1:9092")\
.option("subscribe", "test_topic")\
.load()\
.select(F.col("value").cast(StringType()))

# Perform word count on streaming DataFrame
wordCount = initDF\
.select(F.explode(F.split(F.col("value"), " "))\
.alias("words"))\
.groupBy("words")\
.count()

# Try "update" and "complete" mode.
wordCount\
.writeStream\
.outputMode("update")\
.option("truncate", False)\
.format("console")\
.start()\
.awaitTermination()


