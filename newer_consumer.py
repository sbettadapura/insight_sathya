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

def inc_key_if_exists(key):
	if redis_conn.keys(key):
		redis_conn.set(key, str(int(redis_conn.get(key)) + 1))
	else:
		redis_conn.set(key, str(0))

def dec_key_if_exists(key):
	if redis_conn.keys(key):
		redis_conn.set(key, str(int(redis_conn.get(key)) - 1))
	else:
		redis_conn.set(key, str(0))

def process_accident(redis_conn, user_req):
	redis_conn.incr("incidents_recorded")
	accident_route_num = user_req[0]
	accident_loc = user_req[1][0]
	accident_ts = user_req[1][1]
	accident_route_id = "incident_route%d" % int(accident_route_num)
	redis_conn.set(accident_route_id, str(accident_loc))
	route_inc_id = "route_incident_num%d" % accident_route_num
	redis_conn.set(route_inc_id, int(redis_conn.get(route_inc_id)) + 1)

def process_accident_dep(redis_conn, user_req):
	accident_route_num = user_req[0]
	accident_loc = user_req[1][0]
	accident_ts = user_req[1][1]
	day, tod = accident_ts.split()
	hist_accident_key = "hist_accident_%s_%d_%d" % \
		(day, accident_route_num, accident_loc)
	hist_users_key = "hist_users_%s_%d_%d_%s" % \
		(day, accident_route_num, accident_loc, tod)
	accident_route_id = "curr:incident_route%d" % int(accident_route_num)
	if user_req[1][2]:
		process_accident_occur(redis_conn, accident_route_num, \
		accident_loc, accident_route_id, hist_accident_key, 
		hist_users_key, tod)
	else:
		accident_duration = int(user_req[1][3])
		process_accident_clear(redis_conn, accident_route_num, \
		accident_loc, accident_route_id, hist_accident_key, \
		hist_users_key, tod, accident_duration)
		
def process_accident_occur(redis_conn, accident_route_num, accident_loc, \
		accident_route_id, hist_accident_key, hist_users_key, tod):

	accidents_on_route = redis_conn.smembers(accident_route_id)
	if accidents_on_route is not None:
		if len(accidents_on_route) > 4:
			return
	else:
		inc_key_if_exists("curr:routes_affected")

	redis_conn.incr("curr:incidents_reported_cnt")
	redis_conn.set(hist_accident_key, ' '.join([tod, ""]))
	redis_conn.sadd(accident_route_id, ' '.join([str(accident_loc), tod]))
	route_inc_id = "curr:route_inc_occur_cnt%d" % accident_route_num
	inc_key_if_exists(route_inc_id)

def process_accident_clear(redis_conn, accident_route_num, accident_loc, \
			accident_route_id, hist_accident_key, \
			hist_users_key, tod, accident_duration):


	accident = redis_conn.srandmember(accident_route_id)
	if incident is None:
		dec_key_if_exists("curr:routes_affected")
		return

	redis_conn.incr("curr:incidents_cleared_cnt")
	redis_conn.srem(accident_route_id, accident)
	route_inc_id = "curr:route_inc_clear_cnt_%d" % accident_route_num
	inc_key_if_exists(route_inc_id)
	route_inc_id = "curr:route_inc_occur_cnt%d" % accident_route_num
	dec_key_if_exists(route_inc_id)

	tod_occur, x = redis_conn.get(hist_key)
	redis_conn.set(hist_accident_key, ' '.join([tod_occur, tod]))
	users_affected, x = redis_conn.get(hist_users_key).split()
	redis_conn.set(hist_users_key, ' '.join([str(users_affected), str(duration)]))

def db_user_register(redis_conn, user_req):
	user_route_num = int(user_req[0])
	users_on_route = "curr:user_on_route_cnt_%d" % user_route_num
	redis_conn.incr(users_on_route, len(user_req) / 3)
	accident_route_id = "curr:incident_route_%d" % user_route_num
	accident_loc_ts  = redis_conn.get(accident_route_id)
	user_recs = user_req[1]
	accident_day = None
	users_affected = 0
	for i in range(0, len(user_recs), 3):
		user_loc = int(user_recs[i])
		user_id = user_recs[i + 1]
		ts = user_recs[i + 2]

		if accident_loc_ts is not None:
			accident_loc, accident_day, accident_time = accident_loc_ts.split()
			if int(accident_loc) - user_loc > 0 and int(accident_loc) - user_loc <= 2:
				route_id = "curr:route_%d" % user_route_num
				redis_conn.sadd(route_id, ' '.join([user_id, ts]))
				route_inc_id = "curr:route_incident_users_%d" % user_route_num
				inc_key_if_exists(route_inc_id)
				redis_conn.incr("curr:users_affected_cnt")
				users_affected += 1

	if accident_day is not None:
		hist_users_key = "hist_users_%s_%d_%d_%s" % (accident_day, user_route_num, accident_loc, accident_time)
		redis_conn.set(hist_users_key, ' '.join([str(users_affected), str(0)]))

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

#user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (int(v['incident_route_num']), (int(v['incident_loc']), v['ts'])))

user_accident = directKafkaStream_accident.map(lambda (k,v): json.loads(v)).map(lambda(v): (int(v['incident_route_num']), (int(v['incident_loc']), v['ts'], v['occur_clear'], int(v['accident_duration']))))
#user_accident.pprint()
user_accident.foreachRDD(lambda rdd: rdd.foreachPartition(processAccident))
#user_accident.mapPartitions(processAccident)

user_pos = directKafkaStream_pos.map(lambda (k,v): json.loads(v)).map((lambda(v):(int(v['user_route_num']), (v['user_loc'], v['user_id'], v['ts']))))
#user_pos.pprint(num=1)
user_keyed = user_pos.combineByKey(lambda v: v, lambda a, b: a + b, lambda a, b : a + b)
#user_keyed = user_pos.reduceByKey(lambda a, b : a + b)
user_keyed.foreachRDD(lambda rdd: rdd.foreachPartition(processUserPos))
#user_keyed.mapPartitions(processUserPos)
#user_keyed.pprint()

stream.start()
stream.awaitTermination()
