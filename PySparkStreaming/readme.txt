Following are the demos for Spark Structured Streaming
The prerequisite of these demos is Hadoop and Hive is running (Use ~/Start-Hadoop-Hive.sh)
Note - If you want to stop above services then run ~/Stop-Hadoop-Hive.sh

1) demo1.py - 
Demo - First Spark Streaming example using rate source and console sink. 
Rate source will auto-generate data which we will then print onto a console.
(./run-demo.sh for starting the application)

2) demo2.py -
Demo - Input Sources - Socket
	1- Before running this streaming application (./run-demo.sh for starting the
	 application)
	run following command on a new terminal window to open the socket
	
  nc -lk 9999
	
  Enter following data on socket line by line
	London Paris NewYork Tokyo
	Mumbai Ohio Delhi London
	Delhi London Tokyo Paris
	2- Observe the terminal window of the application

 Note: Make sure you have the socket open before you start the streaming application.

3) demo3.py -
Demo - Input Sources - File
Follow the following given process for running this demo 
	1- Let’s start our streaming application first in a new terminal window, it waits 		for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
	(Use command ./run-demo.sh for starting the application)
	2- Copy files in the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ to simulate streaming.
	3- After putting every file, Observe the terminal window of the application.

Sequence for putting the files
	MSFT_2017.csv
	GE_2017.csv
	MSFT_2016.csv
	AMZN_2017.csv

4) demo4.py -
Demo - Input Sources - Kafka
This demo will read data from Kafka and display it on the console. 
In order to read data from Kafka, first we need to set up Kafka and 
publish messages to a Kafka topic which we will then read into Spark Streaming

Follow the given following process for running this demo
	1- Open a new terminal and run Zookeeper by giving following command
	~/run-kafka_zookeeper_server.sh -s start (It will block the terminal window)
	2- Open a new terminal and run Kafka broker by giving following command
	~/run-kafka_server.sh -s start (It will block the terminal window)
	3- Open a new terminal and run following command to create a kafka topic
 	test_topic.
	./run-kafka.sh 
	4- Let’s start our streaming application first on a new terminal window 
	  (./run-demo.sh for starting the application. Please see the shell
	  comments before running)
	5- On a new terminal window give following command to start kafka producer
	kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test_topic 
	6- Enter following data
	>London Paris NewYork Tokyo
	>Mumbai Ohio Delhi London
	>Delhi London Tokyo Paris
	7- Observe the terminal window of the application

5) demo5.py -
This demo will count the json records placed in a Kafka topic test_topic by a 
Kafka Producer(kafka-console-producer.sh).
Follow the given fllowing process for running this demo
	1- On a new terminal Run ./run-kafka.sh to create a topic (Please see the shell
 	comments before running)
	2- On a new terminal Run the application demo5.py by giving a command ./demo5.py
	3- Run the Kafkar Producer on a new terminal window. Use following command
	kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic test_topic
	4- Copy one json object at a time from ./data/part-00000-85abfdb2-c49e-46c0-8548-e1a7ff6711fd-c000.json to terminal window of kafka-console-producer.sh
	5- Observe the terminal window of the application

6) demo6.py -
Demo - Output sink — Console
Follow the following given process for running this demo 
	1- Let’s start our streaming application first in a new terminal window, it waits 		for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
	(Use command ./run-demo.sh for starting the application)
	2- Copy files in the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ to simulate streaming.
	3- After putting every file, Observe the terminal window of the application.

Sequence for putting the files
	MSFT_2017.csv
	GE_2017.csv
	MSFT_2016.csv
	AMZN_2017.csv

7) demo7.py -
Demo - Output sink — File
This demo will select stock ticker data for symbols like GOOGL, AMZN, AAPL, MSFT.
From Local file system in csv format and put the selected features DF data in parquet
format on HDFS
Follow the following given process for running this demo 
	1- Let’s start our streaming application first in a new terminal window, it 
	waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
	(Use command ./run-demo.sh for starting the application)
	2- Copy files in the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ to simulate streaming.
	3- After putting every file, List the contents of output/filesink_output/
	on HDFS.

Sequence for putting the files
	MSFT_2017.csv
	GE_2017.csv
	MSFT_2016.csv
	AMZN_2017.csv

8) demo8.py
Demo - Output sink — Kafka
Follow the given fllowing process for running this demo
	1- On a new terminal Run ./run-kafka.sh to create a topic (Please see the shell
 	comments before running)
	2- Run the Kafka Consumer on a new terminal window. Use following command
	kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test_topic
	3- On a new terminal Run the application demo8.py by giving a command ./run
	-demo.sh
	4- On a new terminal copy files in the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ to simulate streaming
	Sequence for putting the files
	MSFT_2017.csv
	GE_2017.csv
	MSFT_2016.csv
	AMZN_2017.csv
	5- Observe the terminal window of Kafkar Consumer
9) demo9.py
Demo - Output sink — foreachbatch
Follow the given fllowing process for running this demo
	 1) Follow the steps in ./demo9_db.sql to create a database training and table
 	 test
	 2) Start our streaming application first, 
	 it waits for data in the /home/talentum/Spark-S-Streaming_Demos/data/stream/
	 folder.
	 3) Copy files in the sequence given below from 
	 /home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	 /home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	 Sequence - 
	 MSFT_2017.csv
	 AAPL_2017.csv
	 4) As soon as we drop a file into data/stream, go to mysql console and verify
	 that table get populated with the data.
10) demo10.py
Demo - Output sink — foreach
Follow the given fllowing process for running this demo
	1) Start our streaming application first, it waits for data in the /home
	/talentum/Spark-S-Streaming_Demos/data/stream/ folder.
	2) Copy files in the sequence given below from /home/talentum/Spark-S
	-Streaming_Demos/data/stocks/ to /home/talentum/Spark-S-Streaming_Demos/data
	/stream to simulate streaming.
	Sequence - 
	MSFT_2017.csv
	AAPL_2017.csv
	3) As soon as we drop a file into data/stream, go to Application console and
	verify that the logic in the process method of a CustomWriter get executed.
11) demo11.py
Demo - Checkpointing
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	MSFT_2017.csv
	AAPL_2017.csv

	2) Now let’s interrupt our streaming application to simulate a failure and 
	start the application again.
	3) Copy files in the sequence given below from /home/talentum/Spark-S
	-Streaming_Demos/data/stocks/ to /home/talentum/Spark-S-Streaming_Demos/data
	/stream to simulate streaming.
	
	Sequence -
	AMZN_2017.csv
	
	Here we see an application started withBatch: 2 since it already processed 
	Batch: 0 and Batch: 1. We also see another file, named 2, under the 
	checkpoint/sources folder on HDFS corresponding to AMZN_2017.csv. 
	This is how our application recovered from a failure.
12) demo12.py
Demo - Trigger
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	MSFT_2017.csv
	AAPL_2017.csv
	AMZN_2017.csv	
13) demo13.py
Demo - Operations - Filter
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	GE_2015.csv
	GE_2016.csv
	GE_2017.csv
14) demo14.py
Demo - Operations - Tumbling window
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	GE_2015.csv
	GE_2016.csv
	GE_2017.csv
15) demo15.py
Demo - Operations - Sliding window
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	MSFT_2015.csv
	MSFT_2016.csv
	MSFT_2017.csv
16) demo16.py
Demo - Operations - GroupBy
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	MSFT_2017.csv
17) demo17.py - Not working(Test in 1. batch mode 2. On higher version of spark)
18) demo18.py - Not working(Test in 1. batch mode 2. On higher version of spark)
19) demo19.py 
Demo - Operations - UDF
Follow the given fllowing process for running this demo
	1) Let’s start our streaming application first, it waits for data in the 
	/home/talentum/Spark-S-Streaming_Demos/data/stream/ folder. Copy files in 
	the sequence given below from 
	/home/talentum/Spark-S-Streaming_Demos/data/stocks/ to 
	/home/talentum/Spark-S-Streaming_Demos/data/stream to simulate streaming.
	
	Sequence - 
	MSFT_2017.csv

	

