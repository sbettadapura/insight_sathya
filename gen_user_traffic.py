#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
import random
from random import randrange
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
	x = randrange(65)
	if x >= 0 and x < 15:
		print "generting accident update"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_ACCIDENT)
	elif x >= 15 and x < 25:
		print "generting user register"
		return gen_req(REQ_TYPE_REGISTER)
	elif x >= 25 and x < 32:
		print "generting user query"
		return gen_req(REQ_TYPE_QUERY)
	elif x >= 32 and x < 42:
		print "generting user sign off"
		return gen_req(REQ_TYPE_SIGNOFF)
	elif x >= 42 and x < 47:
		print "generting user update other"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_OTHER)
	elif x >= 45 and x < 65:
		print "generting user update clear"
		return gen_sub_req(REQ_TYPE_UPDATE, SUB_REQ_TYPE_UPDATE_CLEAR)

#This class will handles any incoming request from
#the browser 
class myHandler(BaseHTTPRequestHandler):
	
	#Handler for the GET requests
	def do_GET(self):
		#print "received GET request"
		d = gen_user_traffic()
		self.send_response(200)
		self.send_header('Content-type','text/html')
		self.end_headers()
		# Send the html message
		self.wfile.write("Hello World !")
		if d is None:
			return
		try:
			future = producer.send('usertraffic', d)
			record_metadata = future.get(timeout=10)
			print record_metadata.topic
			print record_metadata.partition
			print record_metadata.offset
		except KafkaError:
			print "Kafka error"
			pass

try:
	#Create a web server and define the handler to manage the
	#incoming request
	server = HTTPServer(('', PORT_NUMBER), myHandler)
	print 'Started httpserver on port ' , PORT_NUMBER
	
	#Wait forever for incoming htto requests
	server.serve_forever()

except KeyboardInterrupt:
	print '^C received, shutting down the web server'
	server.socket.close()
