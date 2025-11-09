#!/usr/bin/python

# Demo - Output sink — foreachBatch
#
# This demo will publish the content of our streaming DataFrame to a MySql DB sink
# using the initDF DataFrame defined.
#
# Note: 
# Perform following activities in sequence for successful execution of the demo
# 1) Follow the steps in ./demo9_db.sql to create a database training and table test
# 2) Start our streaming application first, 
# it waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
# 3) Copy files in the sequence given below from 
# /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
# /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
# Sequence - 
# MSFT_2017.csv
# AAPL_2017.csv
# 4) As soon as we drop a file into data/stream, go to mysql console and verify that
# table get populated with the data
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
.appName("Sink foreachBatch")\
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

resultDf = initDF.select(initDF.Name,initDF.Date,initDF.Open.cast("string"))

def saveToMySql(df, batchId):
	url = """jdbc:mysql://talentum-virtual-machine:3306/training?useSSL=false"""
	df.withColumn("batchId", F.lit(batchId))\
        .write.format("jdbc")\
        .option("url", url)\
        .option("dbtable", "test")\
        .option("user", "bigdata")\
        .option("password", "Bigdata@123")\
        .mode("append")\
        .save()

# Output to MySql
resultDf\
.writeStream\
.outputMode("append")\
.foreachBatch(saveToMySql)\
.start()\
.awaitTermination()


