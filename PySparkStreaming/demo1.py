#!/usr/bin/python

# First Spark Streaming example using rate source and console sink. 
# Rate source will auto-generate data which we will then print onto a console.

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder\
.appName("Rate Source").enableHiveSupport().getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

initDF = spark.readStream.format("rate").option("rowsPerSecond", 1).load()
print(initDF.isStreaming)

resultDF = initDF.withColumn("result", F.col("value") + F.lit(1))

query = resultDF.writeStream.outputMode("append")\
.option("truncate", False).format("console")\
.start()

query.awaitTermination()

