#!/usr/bin/python

# Demo - Operations - Stream-Stream Join (Demo Not working on pyspark 2.4.5, need to check
#
# With stream-batch joins, we can join a batch DataFrame with a streaming DataFrame.
#
# Will perform a simple aggregation operation using groupBy to find the
# year-wise maximum HIGH price for each stock
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
# 
# 
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
.appName("Operation - stream-stream join")\
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
.option("path", "file://" + filePath)\
.schema(compSchema)\
.load()\
.withColumn("Name", getFName())

streamDf1 = initDF.select("Name", "Date", "High", "Low")
streamDf2 = initDF.select("Name", "Date", "Open", "Close")

# Check if DataFrame is streaming or Not.
print("streamDf1 Streaming DataFrame : " + str(streamDf1.isStreaming))
print("streamDf2 Streaming DataFrame : " + str(streamDf2.isStreaming))

joinDf = streamDf1.join(streamDf2, ["Name", "Date"])
#joinDf.select("Name", "Date", "High", "Low", "Close", "Open")

joinDf\
.writeStream\
.outputMode("append")\
.format("console")\
.option("truncate", False)\
.start()\
.awaitTermination()


