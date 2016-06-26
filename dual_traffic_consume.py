import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

import json
import redis
import sys
import datetime
import random
from random import randrange

TS_FMT = '%Y-%m-%d %H:%M:%S'

def init_route_inc_ids(redis_conn):
	for i in range(100):
		route_inc_id = "route_incident_num%d" % i
		redis_conn.set(route_inc_id, 0)

def processAccident(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		process_accident(redis_conn, record)

def dummyprocessUserPos(iter):
	pass

def processUserPos(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		create_db_recs(redis_conn, record)

def create_db_recs(redis_conn, user_req):
	db_user_register(redis_conn, user_req)

def notify_users(users):
	pass

def notify_single_user(user):
	pass

def process_accident(redis_conn, user_req):
	redis_conn.incr("incidents_recorded")
	accident_route_num = user_req[0]
	accident_loc = user_req[1][0]
	accident_ts = user_req[1][1]
	accident_route_id = "incident_route%d" % int(accident_route_num)
	redis_conn.set(accident_route_id, str(accident_loc))
	route_inc_id = "route_incident_num%d" % accident_route_num
	redis_conn.set(route_inc_id, int(redis_conn.get(route_inc_id)) + 1)

def db_user_register(redis_conn, user_req):
	user_route_num = int(user_req[0])
	accident_route_id = "incident_route%d" % user_route_num
	accident_loc = redis_conn.get(accident_route_id)
	user_recs = user_req[1]
	for i in range(0, len(user_recs), 3):
		user_loc = int(user_recs[i])
		user_id = user_recs[i + 1]
		ts = user_recs[i + 2]

		#print "In db_user_register, accident_route_id = ", accident_route_id
		if accident_loc is not None and int(accident_loc) - user_loc > 0 and int(accident_loc) - user_loc <= 2:
			route_id = "route%d" % user_route_num
			redis_conn.sadd(route_id, ' '.join([user_id, ts]))
			route_inc_id = "route_incident_users%d" % user_route_num
			redis_conn.set(route_inc_id, int(redis_conn.get(route_inc_id)) + 1)
			redis_conn.incr("num_users_affected")

sample_secs = int(sys.argv[1])
redis_host = sys.argv[2]
spark_host = sys.argv[3]
conf = SparkConf() \
    .setAppName("PySpark Redis Test") \
    .setMaster("spark://"+spark_host+":7077")

redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
init_route_inc_ids(redis_conn)
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, sample_secs) # window

metalist = ','.join([ip + ":9092" for ip in sys.argv[3:]])
directKafkaStream_accident = KafkaUtils.createDirectStream(stream, ["user_accident"], {"metadata.broker.list": metalist})
directKafkaStream_pos = KafkaUtils.createDirectStream(stream, ["user_pos"], {"metadata.broker.list": metalist})

user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (int(v['incident_route_num']), (int(v['incident_loc']), v['ts'])))
user_accident.pprint(num=1)
user_accident.mapPartitions(processAccident)

user_pos = directKafkaStream_pos.map(lambda (k,v): json.loads(v)).map((lambda(v):(int(v['user_route_num']), (v['user_loc'], v['user_id'], v['ts']))))
#user_pos.pprint(num=1)
user_keyed = user_pos.combineByKey(lambda v: v, lambda a, b: a + b, lambda a, b : a + b)
#user_keyed = user_pos.reduceByKey(lambda a, b : a + b)
#user_keyed.foreachRDD(lambda rdd: rdd.foreachPartition(dummyprocessUserPos))
user_keyed.pprint(num=1)
user_keyed.mapPartitions(processUserPos)

stream.start()
stream.awaitTermination()
