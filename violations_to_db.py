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

bool_columns = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 27]	
column_names = ["date_of_stop","time_of_stop","agency","subagency",\
				"description","location","latitude","longitude",\
				"accident","belts","personal_injury","property_damage",\
				"fatal","commercial_license","hazmat","commercial_vehicle",\
				"alcohol","work_zone","state","vehicletype","year",\
				"make","model","color","violation_type","charge",\
				"article","contributed_to_accident","race","gender",\
				"driver_city","driver_state","dl_state","arrest_type",
				"geolocation"]

def csv_to_dict(line):
	if line.endswith(",") and not line.endswith(",,"):
		line += ","
	items = line.split(',')
	items[3] += ',' + ' ' + items[4]
	items[4:] = items[5:]
	if items[-1] == '':
		items.pop()
	if items[-1] != '':
		items[-2] += ',' + ' ' + items[-1]
		items.pop()

	for i in range(len(items)):
		if i in bool_columns:
			if items[i].lower() == 'yes':
				items[i] = True		
			else:
				items[i] = False		
		if items[i] == '':
			items[i] = None
	return dict(zip(column_names, items))

conf = SparkConf() \
    .setAppName("PySpark Cassandra Test") \
    .setMaster("spark://"+sys.argv[1]+":7077") \
    .set("spark.cassandra.connection.host", sys.argv[1])
sc = SparkContext(conf=conf)

file = sc.textFile("hdfs://" + sys.argv[1] + ":9000/"+sys.argv[2])
lines = file.map(csv_to_dict)
print "dictionaries from csv"
for line in lines.collect():
	print line
lines.saveToCassandra("insight_sathya", "violations")
