#!/usr/bin/python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
from random import randrange
import sys
import time
import json

TS_FMT = '%Y-%m-%d %H:%M:%S'
REQ_TYPE_REGISTER, REQ_TYPE_SIGNOFF, REQ_TYPE_QUERY, REQ_ADD_ROUTE, REQ_TYPE_UPDATE = range(5)

SUB_REQ_TYPE_UPDATE_ACCIDENT, SUB_REQ_TYPE_UPDATE_OTHER, SUB_REQ_TYPE_UPDATE_CLEAR, SUB_REQ_TYPE_UPDATE_POS = range(4)

PORT_NUMBER = 8180
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))


def gen_req(req):
	ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	user_data = {"req_type": req, "ts": ts}
	return user_data


def gen_sub_req(req, sub_req):
	ts = datetime.datetime.now().strftime(TS_FMT)
	req = REQ_TYPE_UPDATE
	user_data = {"req_type": req, "sub_req_type": sub_req, "ts": ts} 
	return user_data

def gen_user_traffic():
	x = randrange(905)
	if x >= 0 and x < 1:
		#print "generting accident update"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_ACCIDENT)
	elif x >= 1 and x < 300:
		#print "generting user register"
		return gen_req(REQ_TYPE_REGISTER)
	elif x >= 300 and x < 600:
		#print "generting user query"
		return gen_req(REQ_TYPE_QUERY)
	elif x >= 600 and x < 900:
		#print "generting user sign off"
		return gen_req(REQ_TYPE_SIGNOFF)
	elif x >= 900 and x < 902:
		#print "generting user update other"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_OTHER)
	elif x >= 902 and x < 905:
		#print "generting user update clear"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_CLEAR)

#This class will handles any incoming request from
#the browser 
def trigger_gen(topic):
	d = gen_user_traffic()
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
	topic = sys.argv[1]
	rate = int(sys.argv[2])
	while True:
		trigger_gen(topic)
		time.sleep(1.0/rate)
