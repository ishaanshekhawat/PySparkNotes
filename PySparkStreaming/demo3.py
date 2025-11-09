#!/usr/bin/python

# Demo - Input Sources - File
#
# This demo will perform aggregation operation on stock ticker data for symbols like GOOGL, AMZN, AAPL, MSFT.
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
# GE_2017.csv
# MSFT_2016.csv
# AMZN_2017.csv

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
.appName("File Source")\
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

# Perform basic aggregation on our streaming DataFrame.
stockDf = initDF\
.groupBy(F.col("Name"), F.year(F.col("Date")).alias("Year"))\
.agg(F.max("High").alias("Max"))

#We can also perform the above transformation using a SQL query
#Register DataFrame as view. 
#initDF.createOrReplaceTempView("stockView")

#Run SQL Query
#query = """select year(Date) as Year, Name, max(High) as Max from stockView group by Name, Year"""
#stockDf = spark.sql(query)

# Output to Console
# Try "update" and "complete" mode.
stockDf\
.writeStream\
.outputMode("update")\
.option("truncate", False)\
.option("numRows", 3)\
.format("console")\
.start()\
.awaitTermination()


