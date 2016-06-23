#!/usr/bin/python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
from random import randrange
import sys
import time
import json

NUM_USERS = 100000
NUM_ROUTES = 100
MAX_ROUTE_LEN = 23
TS_FMT = '%Y-%m-%d %H:%M:%S'
route_dict = dict()

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

def gen_route_info():
	rt_start = 0
	for i in range(NUM_ROUTES):
		rt_len = randrange(MAX_ROUTE_LEN/2, MAX_ROUTE_LEN)
		route_dict[i] = (rt_start, rt_start + rt_len)
		rt_start += rt_len

def gen_update_pos():
	ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	rand_route_num = randrange(NUM_ROUTES)
	start,end = list(route_dict[rand_route_num])
	rand_loc = randrange(start, end)
	user_id = "user%d" % randrange(NUM_USERS)
	user_data = {"user_route_num" : rand_route_num, "user_loc": rand_loc, "user_id" : user_id, "ts": ts}
	return user_data


def gen_update_accident():
	ts = datetime.datetime.now().strftime(TS_FMT)
	rand_route_num = randrange(NUM_ROUTES)
	start,end = list(route_dict[rand_route_num])
	rand_loc = randrange(start, end)
	user_data = {"incident_route_num" : rand_route_num, "incident_loc" : rand_loc, "ts": ts} 
	return user_data

def gen_user_traffic():
	x = randrange(100)
	if x >= 0 and x < 2:
		#print "generting accident update"
		return ("user_accident", gen_update_accident())
	elif x >= 2 and x < 100:
		#print "generting user register"
		return ("user_pos", gen_update_pos())

#This class will handles any incoming request from
#the browser 
def trigger_gen():
	topic, d = gen_user_traffic()
	try:
		future = producer.send(topic, d)
		#record_metadata = future.get(timeout=10)
		#print record_metadata.topic
		#print record_metadata.partition
		#print record_metadata.offset
	except KafkaError:
		print "Kafka error"
		pass
if __name__ == "__main__":
	rate = int(sys.argv[1])
	gen_route_info()
	while True:
		trigger_gen()
		time.sleep(1.0/rate)
