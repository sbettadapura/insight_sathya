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
NUM_ROUTES = 100
MAX_ROUTE_LEN = 23

REQ_TYPE_REGISTER, REQ_TYPE_SIGNOFF, REQ_TYPE_QUERY, REQ_ADD_ROUTE, REQ_TYPE_UPDATE = range(5)

SUB_REQ_TYPE_UPDATE_ACCIDENT, SUB_REQ_TYPE_UPDATE_OTHER, SUB_REQ_TYPE_UPDATE_CLEAR, SUB_REQ_TYPE_UPDATE_POS = range(4)

def sendPartition(iter):
	redis_conn = redis.StrictRedis(host = redis_host, port = 6379)
	for record in iter:
		print record
		redis_conn.incr("num_recs_recvd")
		create_db_recs(redis_conn, record)

def create_db_recs(redis_conn, user_req):
	req_type = user_req['req_type']
	if req_type == REQ_TYPE_REGISTER:
		db_user_register(redis_conn, user_req)
	elif req_type == REQ_TYPE_SIGNOFF:
			db_user_signoff(redis_conn, user_req)
	elif req_type == REQ_TYPE_QUERY:
			db_user_query(redis_conn, user_req)
	elif req_type == REQ_TYPE_UPDATE:
		sub_req_type = user_req["sub_req_type"]
		if sub_req_type == SUB_REQ_TYPE_UPDATE_CLEAR:
			db_incident_clear(redis_conn, user_req)
		else:
			db_incident_update(redis_conn, user_req, sub_req_type)

def notify_user(users):
	pass

def notify_single_user(user):
	pass

def get_num_users(redis_conn):
	if redis_conn.exists("users"):
		return redis_conn.scard("users")
	else:
		return 0

def get_num_routes(redis_conn):
	return redis_conn.llen("route_info")

def gen_route_info(redis_conn):
	rt_start = 0
	for i in range(NUM_ROUTES):
		rt_len = randrange(MAX_ROUTE_LEN/2, MAX_ROUTE_LEN)
		redis_conn.rpush("route_info", ' '.join([str(rt_start), str(rt_start + rt_len)]))
		rt_start += rt_len


def get_route_endpoints(redis_conn, route_num):
	x = redis_conn.lindex("route_info", route_num)
	return map(int, x.split())

def db_user_register(redis_conn, user_req):
	user_id = "user%d" % get_num_users(redis_conn)
	route_num = randrange(get_num_routes(redis_conn))
	route_id = "route%d" % route_num
	ts = user_req['ts']
	route_start, route_end = get_route_endpoints(redis_conn, route_num)
	start = randrange(route_start, route_end)
	end = randrange(start, route_end)

	redis_conn.set(user_id, ' '.join([str(route_num), str(start)]))
	redis_conn.sadd(route_id, user_id)
	redis_conn.sadd("users", ' '.join([user_id, ts]))

def db_user_signoff(redis_conn, user_req):
	now_ts = datetime.datetime.now().strftime(TS_FMT)
	user_id, ts1, ts2 = redis_conn.srandmember("users").split()
	ts = ts1 + ' ' + ts2
	route_num, loc = map(int, redis_conn.get(user_id).split())
	user_time = (datetime.strptime(now_ts, (TS_FMT)) - \
				datetime.strptime(ts, (TS_FMT))).total_seconds()
	route_id = "route%d" % route_num
	redis_conn.srem(route_id, user_id)
	redis_conn.srem("users", ' '.join([user_id, ts]))

def db_user_query(redis_conn, user_req):
	route_num = randrange(get_num_routes(redis_conn))
	route_start, route_end = get_route_endpoints(redis_conn, route_num)
	start = randrange(route_start, route_end)
	end = randrange(start, route_end)
	if redis_conn.sismember("route_incidents", str(route_num)):
		if start <= loc and loc <= end:
			user_to_be_notified = redis_conn.srandmember("route%d" % route_num)
			notify_single_user(user_to_be_notified)

def db_incident_update(redis_conn, user_req):
	user_id, ts1, ts2 = redis_conn.srandmember("users").split()
	ts = ts1 + ' ' + ts2
	rand_route_num = randrange(get_num_routes(redis_conn))
	rand_route_id = "route%d" % rand_route_num
	route_start, route_end = get_route_endpoints(redis_conn, rand_route_num)
	rand_loc = randrange(route_start, route_end)
	sub_update_type = random.choice[REQ_UPDATE_ACCIDENT, ROUTE_UPDATE_OTHER]
	ts = datetime.datetime.now().strftime(TS_FMT)
	redis_conn.sadd("incidents", \
			' '.join([user_id, str(rand_route_num), str(rand_loc), \
				str(sub_update_type), ts]))
	redis_conn.sadd("route_incidents", str(route_num))
	users_on_route = redis_conn.smembers(rand_route_id)
	users_to_be_notified = set()
	for user in users_on_route:
		route_num, loc = redis_conn.get(user_id)
		if rand_loc - loc >= 1:
			users_to_be_notified.add(user)
	notify_users(users_to_be_notified)

def db_incident_clear(redis_conn, user_req):
	now_ts = datetime.datetime.now().strftime(TS_FMT)
	found_rand_incident = False
	while not found_rand_incident:
		rand_route_num = redis_conn.srandmember("route_incidents")
		for y in redis_conn.smembers("incidents"):
			x = y.split()
			user_id = x[0]
			route_num = int(x[1])
			rand_loc = int(x[2])
			sub_update_tye = int(x[3])
			ts = ts1 + ' ' + ts2
			if route_num == rand_route_num and \
				datetime.strptime(now_ts, (TS_FMT)) - \
				datetime.strptime(ts, (TS_FMT)) > datetime.timedelta(0, 60):
				found_rand_incident = True
				break
	redis_conn.srem("incidents", \
			' '.join([user_id, str(route_num), str(rand_loc), \
				str(sub_update_type), ts]))
	redis_conn.srem("route_incidents", str(rand_route_num))


redis_host = sys.argv[1]
conf = SparkConf() \
    .setAppName("PySpark Redis Test") \
    .setMaster("spark://"+redis_host+":7077")

# generate a bunch of routes
redis_conn = redis.StrictRedis(host = redis_host, port = 6379)
gen_route_info(redis_conn)
# set up our contexts
#sc = CassandraSparkContext(conf=conf)
sc = SparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window

"""
kafka_stream = KafkaUtils.createStream(stream, \
                                       "localhost:2181", \
                                       "raw-event-streaming-consumer",
                                        {"user_traffic":1})
"""

directKafkaStream = KafkaUtils.createDirectStream(stream, ["user_traffic"], {"metadata.broker.list": "ec2-52-37-251-31.us-west-2.compute.amazonaws.com:9092, ec2-52-40-170-121.us-west-2.compute.amazonaws.com:9092, ec2-52-40-108-38.us-west-2.compute.amazonaws.com:9092, ec2-52-37-124-123.us-west-2.compute.amazonaws.com:9092"})
user_req = directKafkaStream.map(lambda (k,v): json.loads(v))
user_req.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

stream.start()
stream.awaitTermination()
