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

def processPartition(iter):
	print "before redis connection"
	redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
	print "after redis connection"
	for record in iter:
		print "before calling create_db_recs"
		create_db_recs(redis_conn, record)
		print "after calling create_db_recs"

def processRecord(record):
	#global POOL
	#redis_conn = redis.Redis(connection_pool=POOL)
	redis_conn = redis.Redis(host = redis_host, port = 6379, db = 0, password = 'noredishackers')
	create_db_recs(redis_conn, record)

def create_db_recs(redis_conn, user_req):
	req_type = user_req['req_type']
	"""
	redis_conn.incr("recs_in", 1)
	x = int(redis_conn.get("recs_in"))
	redis_conn.set("req_type %d" % x, req_type)
	"""
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
			#db_incident_clear_dummy(redis_conn, user_req)
		else:
			db_incident_update(redis_conn, user_req, sub_req_type)

def notify_users(users):
	pass

def notify_single_user(user):
	pass

def get_num_users(redis_conn):
	if redis_conn.keys("users_cnt"):
		return int(redis_conn.get("users_cnt"))
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
	redis_conn.incr("calls:users_register")
	user_id = "user%d" % get_num_users(redis_conn)
	route_num = randrange(get_num_routes(redis_conn))
	route_id = "route%d" % route_num
	ts = user_req['ts']
	route_start, route_end = get_route_endpoints(redis_conn, route_num)
	start = randrange(route_start, route_end)
	end = randrange(start, route_end)

	redis_conn.set(user_id, ' '.join([str(route_num), str(start)]))
	redis_conn.incr("num_userid")
	redis_conn.sadd(route_id, user_id)
	redis_conn.incr("num_routeid")
	redis_conn.sadd("users", ' '.join([user_id, ts]))
	redis_conn.incr("num_users")

def db_user_signoff(redis_conn, user_req):
	#now_ts = datetime.datetime.now().strftime(TS_FMT)
	x = redis_conn.srandmember("users")
	if x is None:
		redis_conn.incr("race:signoff:users")
		return
	user_id, ts1, ts2 = x.split()
	x = redis_conn.get(user_id)
	if x is None:
		redis_conn.incr("race:singoff:userid")
		return
	redis_conn.incr("calls:users_signoff")
	route_num, loc = map(int, x.split())
	"""
	user_time = (datetime.strptime(now_ts, (TS_FMT)) - \
				datetime.strptime(ts, (TS_FMT))).total_seconds()
	"""
	route_id = "route%d" % route_num
	redis_conn.srem(route_id, user_id)
	redis_conn.decr("num_routeid")
	redis_conn.srem("users", ' '.join([user_id, ts1, ts2]))
	redis_conn.decr("num_users")
	redis_conn.delete(user_id)
	redis_conn.decr("num_userid")

def db_user_query(redis_conn, user_req):
	redis_conn.incr("calls:user_query")
	route_num = randrange(get_num_routes(redis_conn))
	route_start, route_end = get_route_endpoints(redis_conn, route_num)
	start = randrange(route_start, route_end)
	end = randrange(start, route_end)
	if redis_conn.sismember("route_incidents", str(route_num)):
		pass
		"""
		if start <= loc and loc <= end:
			user_to_be_notified = redis_conn.srandmember("route%d" % route_num)
			notify_single_user(user_to_be_notified)
		"""

def db_incident_update(redis_conn, user_req, sub_req_type):
	x = redis_conn.srandmember("users")
	if x is None:
		return
	user_id, ts1, ts2 = x.split()
	x = redis_conn.get(user_id)
	if x is None:
		redis_conn.incr("race:update:userid")
		return
	redis_conn.incr("calls:incident_update")
	rand_route_num, rand_loc = map(int, x.split())
	#now_ts = datetime.datetime.now().strftime(TS_FMT)
	redis_conn.sadd("incidents", \
			' '.join([user_id, str(rand_route_num), str(rand_loc), \
				str(sub_req_type), ts1, ts2]))
	redis_conn.incr("num_incidents")
	redis_conn.sadd("route_incidents", str(rand_route_num))
	redis_conn.incr("num_route_incidents")
	rand_route_id = "route%d" % rand_route_num
	users_on_route = redis_conn.smembers(rand_route_id)
	users_to_be_notified = set()
	for user in users_on_route:
		route_num, loc = map(int, redis_conn.get(user_id).split())
		if rand_loc - loc >= 1:
			users_to_be_notified.add(user)
	notify_users(users_to_be_notified)
	redis_conn.incr("stats:incident_update")

def db_incident_clear_dummy(redis_conn, user_req):
	pass
def db_incident_clear(redis_conn, user_req):
	#now_ts = datetime.datetime.now().strftime(TS_FMT)
	redis_conn.incr("calls:incident_clear")
	found_rand_incident = False
	noluck = False
	while not found_rand_incident:
		rand_route_num = redis_conn.srandmember("route_incidents")
		if rand_route_num is None:
			return
		else:
			rand_route_num = int(rand_route_num)
		for y in redis_conn.smembers("incidents"):
			x = y.split()
			user_id = x[0]
			route_num = int(x[1])
			rand_loc = int(x[2])
			sub_update_type = int(x[3])
			ts1 = x[4]
			ts2 = x[5]
			"""
			if route_num == rand_route_num and \
				datetime.strptime(now_ts, (TS_FMT)) - \
				datetime.strptime(ts, (TS_FMT)) > datetime.timedelta(0, 60):
				found_rand_incident = True
			"""
			if route_num == rand_route_num:
				found_rand_incident = True
				break
		else:
			found_rand_incident = True
			noluck = True
	if not noluck:
		redis_conn.srem("incidents", \
			' '.join([user_id, str(route_num), str(rand_loc), \
				str(sub_update_type), ts1, ts2]))
		redis_conn.decr("num_incidents")
		redis_conn.srem("route_incidents", str(rand_route_num))
		redis_conn.decr("num_route_incidents")
	else:
		redis_conn.incr("noluck_ctr")
		print "noluck ", rand_route_num


mytopic = sys.argv[1]
redis_host = sys.argv[2]
spark_host = sys.argv[3]
conf = SparkConf() \
    .setAppName("PySpark Redis Test") \
    .setMaster("spark://"+spark_host+":7077")

# generate a bunch of routes
#POOL = redis.ConnectionPool(host='ec2-52-37-251-31.us-west-2.compute.amazonaws.com', port=6379, db=0)
redis_conn = redis.Redis(host = redis_host, port = 6379, password = 'noredishackers')
redis_conn.delete("faultering_userid")
redis_conn.delete("race:update:users")
redis_conn.delete("race:update:userid")
redis_conn.delete("race:signoff:users")
redis_conn.delete("race:singoff:userid")
gen_route_info(redis_conn)
# set up our contexts
#sc = CassandraSparkContext(conf=conf)
sc = SparkContext(conf=conf)
stream = StreamingContext(sc, 1) # 1 second window

metalist = ','.join([ip + ":9092" for ip in sys.argv[3:]])
directKafkaStream = KafkaUtils.createDirectStream(stream, [mytopic], {"metadata.broker.list": metalist})
user_req = directKafkaStream.map(lambda (k,v): json.loads(v))
#user_req.foreachRDD(lambda rdd: rdd.foreach(processRecord))
user_req.foreachRDD(lambda rdd: rdd.foreachPartition(processPartition))

stream.start()
stream.awaitTermination()
