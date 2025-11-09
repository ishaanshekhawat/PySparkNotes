#!/usr/bin/python

# Demo - Output sink — Kafka
#
# This demo will publish the content of our streaming DataFrame to a Kafka topic sink
# using the initDF DataFrame defined.
#
# Note: 
# Let’s start our streaming application first, 
# it waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
# Start kafka-console-consumer to listen the topic test_topic (kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic)
# Copy files in the sequence given below from 
# /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
# /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
# Sequence - 
# MSFT_2017.csv
# AAPL_2017.csv
# As soon as we drop a file into data/stream, we see our transformed output on Kafka’s console (kafka-console-consumer)
#

# Import Libraries
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import os
import glob

# Create Spark Session
spark = SparkSession\
.builder\
.master("local")\
.appName("Sink Kafka")\
.getOrCreate()

# Get the logger and set the log level
spark.sparkContext.setLogLevel("WARN")

# Define Schema
compSchema = StructType([
	StructField("Date", StringType(), True),
	StructField("Open", DoubleType(), True),
	StructField("High", DoubleType(), True),
	StructField("Low", DoubleType(), True),
	StructField("Close", DoubleType(), True),
	StructField("Adjusted Close", DoubleType(), True),
	StructField("Volume", DoubleType(), True)
])

filePath = "/home/talentum/Spark-S-Streaming_Demos/data/stream/"
# Extract the Name of the stock from the file name.
def getFileName() : 
	path = os.path.abspath(filePath)
	#dir_list = os.listdir(path)
	list_of_files = glob.glob(path + '/*') # * means all if need specific format then *.csv
	latest_file = max(list_of_files, key=os.path.getctime)
	#name = dir_list[0].split("_")[0]
	name = latest_file.split("/")[-1].split("_")[0]
	return name

# Create a udf
getFName = F.udf(getFileName, StringType())

# Create Streaming DataFrame by reading data from File Source.
initDF = spark\
.readStream\
.format("csv")\
.option("maxFilesPerTrigger", 2)\
.option("header", True)\
.option("path", "file://" + filePath)\
.schema(compSchema)\
.load()\
.withColumn("Name", getFName())

resultDf = initDF.select(F.concat_ws("|", initDF.Name,initDF.Date,initDF.High.cast("string"),
initDF.Low.cast("string"),initDF.Open.cast("string"),
initDF.Close.cast("string")).alias("value"))

# Output to Kafka
resultDf\
.writeStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "127.0.0.1:9092")\
.option("topic", "test_topic")\
.option("checkpointLocation", "checkpoint/kafka_checkpoint")\
.start()\
.awaitTermination()


