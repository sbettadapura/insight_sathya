#!/usr/bin/python
from kafka import KafkaProducer, KeyedProducer
from kafka.errors import KafkaError
import datetime
from random import randrange
import time
import json

NUM_PARTITIONS = 18
NUM_USERS = 400000
NUM_ROUTES = 400
MAX_ROUTE_LEN = 23
TS_FMT = '%Y-%m-%d %H:%M:%S'
MIN_ACCIDENT_DURATION = 5
MAX_ACCIDENT_DURATION= 10
route_dict = dict()
user_location = dict()
route_accidents = dict()

def gen_route_info():
	rt_start = 0
	for i in range(NUM_ROUTES):
		rt_len = randrange(MAX_ROUTE_LEN/2, MAX_ROUTE_LEN)
		route_dict[i] = (rt_start, rt_start + rt_len)
		rt_start += rt_len

def gen_user_exit():
	rand_user_id = user_location.keys()[randrange(len(user_location))]
	del user_location[rand_user_id]
	return (None, None)
def gen_user_pos():
	user_found = False
	ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	while not user_found:
		user_id = "user_%d" % randrange(NUM_USERS)
		if user_id in user_location:
			user_route_num, user_loc = user_location[user_id]
			start, end = route_dict[user_route_num]
			rand_loc = randrange(user_loc, end)
			if rand_loc - user_loc < 3:
				rand_route_num = user_route_num
				user_found = True
		else:
			rand_route_num = randrange(NUM_ROUTES)
			start,end = list(route_dict[rand_route_num])
			rand_loc = randrange(start, end)
			user_found = True

	user_location[user_id] = (rand_route_num, rand_loc)
	user_data = {"user_route_num" : rand_route_num, "user_loc": rand_loc, "user_id" : user_id, "ts": ts}
	kafka_key = rand_route_num % NUM_PARTITIONS
	return (kafka_key, user_data)

def gen_accident_occur():
	found_new_route = False
	while not found_new_route:
		rand_route_num = randrange(NUM_ROUTES)
		found_new_route =  rand_route_num not in route_accidents

	ts = datetime.datetime.now().strftime(TS_FMT)
	start, end = route_dict[rand_route_num]
	rand_loc = randrange(start, end)

	user_data = {"incident_route_num" : rand_route_num, "incident_loc" : rand_loc, "ts": ts, "occur_clear": 1, "accident_duration": 0} 
	kafka_key = rand_route_num % NUM_PARTITIONS
	route_accidents[rand_route_num] = (rand_loc, ts)
	return (kafka_key, user_data)

def gen_accident_clear():
	found_route = False
	if len(route_accidents) == 0:
		return (None, None)
	while not found_route:
		accident_route_num = route_accidents.keys()[randrange(len(route_accidents))]
		accident_loc, accident_ts = route_accidents[accident_route_num]
		rand_duration = randrange(MIN_ACCIDENT_DURATION, MAX_ACCIDENT_DURATION)
		now = datetime.datetime.strptime(accident_ts, TS_FMT) + datetime.timedelta(seconds=rand_duration)
		now_ts = datetime.datetime.strftime(now, TS_FMT)
		found_route = True

	del route_accidents[accident_route_num]
	user_data = {"incident_route_num" : accident_route_num, \
		 "incident_loc" : accident_loc, "ts": now_ts, \
		"occur_clear": 0, "accident_duration": rand_duration} 
	kafka_key = accident_route_num % NUM_PARTITIONS
	return (kafka_key, user_data)

def gen_user_traffic():
	x = randrange(100)
	if x >= 0 and x < 2:
		kafka_key, data = gen_accident_occur()
		return ("user_accident", data, kafka_key)
	if x >= 2 and x < 4:
		kafka_key, data = gen_accident_clear()
		return ("user_accident", data, kafka_key)
	if x >= 4 and x < 6:
		kafka_key, data = gen_user_exit()
		return ("user_accident", data, kafka_key)
	elif x >= 6 and x < 100:
		kafka_key, data = gen_user_pos()
		return ("user_pos", data, kafka_key)

def trigger_gen(producer, keyed):
	topic, d, kafka_key = gen_user_traffic()
	if d is None:
		return
	try:

		print d
		if keyed:
			future = producer.send(topic, d, key = str(kafka_key))
		else:
			future = producer.send(topic, d)
	except KafkaError:
		print "Kafka error"
		pass


if __name__ == "__main__":
	import sys
	keyed = int(sys.argv[1])
	servers = ','.join([ip + ":9092" for ip in sys.argv[2:]])
	producer = KafkaProducer(bootstrap_servers = servers, value_serializer=lambda m: json.dumps(m).encode('ascii'))
	gen_route_info()
	while True:
		trigger_gen(producer, keyed)
