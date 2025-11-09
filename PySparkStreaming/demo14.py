#!/usr/bin/python

# Demo - Tumbling window
#
# This is a demo of Tumbling window 
# Window operations are very similar to groupBy operations. In groupBy, aggregation is
# based on the specified group or key while in window operations aggregation is based on
# event windows. Spark supports 2 types of windows Tumbling window and Sliding window.
#
# Tumbling windows are non-overlapping which means each data point will be part of only
# one window.
# 
# Note: 
# Let’s start our streaming application first, 
# it waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
# Copy files in the sequence given below from 
# /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
# /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
#
# Sequence - 
# GE_2017.csv
# GE_2016.csv
# GE_2015.csv
# 
# We can check the size of the window in the window column and can see that none of the
# windows overlap. You can change the duration of the window and check the result
# yourself as well.
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
.appName("Operation - Tumbling window")\
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

# Perform tumbling window agreegation operation on streaming DataFrame.
resultDf = initDF.select("Name", "Date", "Open", "High", "Low")\
.groupBy(F.window(F.col('Date'), "10 days"), F.col("Name"))\
.agg({"High": "max"})

resultDf.orderBy(resultDf.window.start)

# File sink only supports append output mode
resultDf\
.writeStream\
.outputMode("complete")\
.option("checkpointLocation", "checkpoint")\
.format("console")\
.option("truncate", False)\
.start()\
.awaitTermination()


