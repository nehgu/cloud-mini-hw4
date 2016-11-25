
# coding: utf-8

# In[5]:

from __future__ import print_function

import sys
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


# In[7]:

sc = SparkContext(appName="TwitterStreamKafkaSparkWordCount")
ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")


# In[8]:

def updateCount(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)


# In[10]:

zkQuorum = "localhost:9902"
topic = "tweets"
twitterStream = KafkaUtils.createDirectStream(ssc, topics = ['tweets'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
tweets = twitterStream.map(lambda x: x[1].encode("ascii","ignore"))
words=tweets.flatMap(lambda x: x.split(" "))
wordfreq=words.map(lambda x: (x,1))
wordCount=wordfreq.reduceByKey(lambda x, y: x + y)
runningCounts = wordfreq.updateStateByKey(updateCount)
runningCounts.pprint()
print(runningCounts)


# In[ ]:

counts = []
wordCount.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))


# In[ ]:

#twitterStream.pprint()
ssc.start()
ssc.awaitTermination()


# In[11]:

#SparkstreamConsumer
#get_ipython().system(u'jupyter nbconvert --to python Untitled.ipynb')


# In[ ]:



