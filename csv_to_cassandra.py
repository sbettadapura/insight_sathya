#!/usr/bin/env python
from pyspark import SparkContext, SparkConf
import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1
import sys

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster("spark://"+sys.argv[1]+":7077") \
    .set("spark.cassandra.connection.host", sys.argv[1])
sc = SparkContext(conf=conf)

file = sc.textFile("hdfs://" + sys.argv[1] + ":9000/"+sys.argv[2])
lines = file.map(lambda x:x.split(',')).map(lambda x: {"textstr": x[0], "numero" : int(x[1])})
for line in lines.collect():
	print line
lines.saveToCassandra("demodb", "csvtodb")
