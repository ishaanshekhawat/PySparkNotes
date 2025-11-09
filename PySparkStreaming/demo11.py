#!/usr/bin/python

# Demo - Checkpointing
#
# This is a demo of checkpointing 
# A checkpoint helps build fault-tolerant and resilient Spark applications.
#
# will select stock ticker data for symbols like GOOGL, AMZN, AAPL, MSFT.
# From Local file system in csv format and shows the selected feature DF on console.
#
# Note: 
# Let’s start our streaming application first, 
# it waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
# Copy files in the sequence given below from 
# /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
# /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
#
# Sequence - 
# MSFT_2017.csv
# AAPL_2017.csv
# Now let’s interrupt our streaming application to simulate a failure and start the
# application again
# Copy files in the sequence given below from 
# /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
# /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
#
# Sequence -
# AMZN_2017.csv
# Here we see an application started withBatch: 2 since it already processed 
# Batch: 0 and Batch: 1. We also see another file, named 2, under the checkpoint/sources
# folder on HDFS corresponding to AMZN_2017.csv. 
# This is how our application recovered from a failure.
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
.appName("Check Point")\
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
.option("maxFilesPerTrigger", 1)\
.option("path", "file://" + filePath)\
.schema(compSchema)\
.load()\
.withColumn("Name", getFName())

# Perform basic aggregation on our streaming DataFrame.
resultDf = initDF.select("Name", "Date", "Open", "High", "Low")\
.groupBy(F.col("Name"), F.year(F.col("Date")).alias("Year"))\
.agg({"High": "avg"}).alias("Avg")\
.withColumn("timestamp", F.current_timestamp())

# File sink only supports append output mode
# Try with "update" mode as well.
resultDf\
.writeStream\
.outputMode("complete")\
.option("checkpointLocation", "checkpoint")\
.format("console")\
.start()\
.awaitTermination()


