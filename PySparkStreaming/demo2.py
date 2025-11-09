#!/usr/bin/python

# Demo - Input Sources - Socket

# Before running this streaming application
# Run following command on a new terminal window to open the socket
# nc -lk 9999
#
# Note: Make sure you have the socket open before you start the streaming application.
#

# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create Spark Session
spark = SparkSession\
.builder\
.master("local")\
.appName("Socket Source")\
.getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

# Define host and port number to Listen.
host = "127.0.0.1"
port = "9999"

# Create Streaming DataFrame by reading data from socket.
initDF = spark\
.readStream\
.format("socket")\
.option("host", host)\
.option("port", port)\
.load()

# Check if DataFrame is streaming or Not.
print("Streaming DataFrame : {} ".format(initDF.isStreaming))
print(initDF.printSchema())

# Perform word count on streaming DataFrame
wordCount = initDF\
.select(F.explode(F.split(F.col("value"), " "))\
.alias("words"))\
.groupBy("words")\
.count()

# Print Schema of DataFrame
print("Schema of DataFame wordCount.")
print(wordCount.printSchema())

# Try "update" and "complete" mode.
wordCount\
.writeStream\
.outputMode("update")\
.option("truncate", False)\
.format("console")\
.start()\
.awaitTermination()


