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

def processPartition(iter):
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	for record in iter:
		create_db_recs(redis_conn, record)

def create_db_recs(redis_conn, user_req):
	db_user_register(redis_conn, user_req)

def notify_users(users):
	pass

def notify_single_user(user):
	pass

def db_user_register(redis_conn, user_req):
	user_route_num = user_req[0]
	user_id = user_req[1][1][1]
	ts = user_req[1][1][2]
  
	route_id = "route%d" % user_route_num
	redis_conn.sadd(route_id, ' '.join([user_id, ts]))
	route_inc_id = "route_incident_num%d" % user_route_num
	redis_conn.set(route_inc_id, int(redis_conn.get(route_inc_id)) + 1)
	redis_conn.incr("num_incidents")

redis_host = sys.argv[1]
spark_host = sys.argv[2]
conf = SparkConf() \
    .setAppName("PySpark Redis Test") \
    .setMaster("spark://"+spark_host+":7077")

redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
init_route_inc_ids(redis_conn)
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, 1) # 1 second window

metalist = ','.join([ip + ":9092" for ip in sys.argv[2:]])
directKafkaStream_accident = KafkaUtils.createDirectStream(stream, ["user_accident"], {"metadata.broker.list": metalist})
directKafkaStream_pos = KafkaUtils.createDirectStream(stream, ["user_pos"], {"metadata.broker.list": metalist})

user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (int(v['incident_route_num']), (int(v['incident_loc']), v['ts'])))
user_accident.pprint()
user_pos = directKafkaStream_pos.map(lambda (k,v): json.loads(v)).map((lambda(v):(int(v['user_route_num']), (int(v['user_loc']), v['user_id'], v['ts']))))
user_pos.pprint()
user_join = user_accident.join(user_pos)
user_join.pprint()
user_filter = user_join.filter(lambda (k, v): v[1][0] > v[0][0] and v[1][0] - v[0][0] <= 2)
user_filter.foreachRDD(lambda rdd: rdd.foreachPartition(processPartition))

stream.start()
stream.awaitTermination()
