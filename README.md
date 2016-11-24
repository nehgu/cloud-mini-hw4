# cloud-mini-hw4

# Spark Streaming
Integrate Spark with Kafka and do some simple processing for streaming data.

# Requirements
1. Install Kafka and Spark. You don't have to set up a cluster. Deploying them on your local machine is fine.
2. Feed real time data to Kafka and then use Spark Streaming to fetch this data and count the frequency of each word and print complete results to the console (You should use "foreachRDD"  method). 

Generally speaking, you should first start your Kafka, submit your job to Spark, then put data into Kafka and the results(word, freq) should be shown in your console.
