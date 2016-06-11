#!/usr/bin/python
from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from kafka import KafkaProducer
from kafka.errors import KafkaError
import datetime
import random
import json

QUERY_SPECIFIC, QUERY_AVG, QUERY_DATE_BASED = range(3)

stock_queries = [(QUERY_SPECIFIC, "Select location from violation where driver_state = 'MD'"), \
(QUERY_AVG, "Select Avg(year) from violation where driver_state = 'MD'"), \
(QUERY_DATE_BASED, "Select location from violation where date_of_stop < '2016-06-10'")]

PORT_NUMBER = 8180
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

#This class will handles any incoming request from
#the browser 
class myHandler(BaseHTTPRequestHandler):
	
	#Handler for the GET requests
	def do_GET(self):
		print "received GET request"
		query = random.choice(stock_queries)
		d = {"ts": str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')), "query_type": query[0], "query_text": query[1]}
		self.send_response(200)
		self.send_header('Content-type','text/html')
		self.end_headers()
		# Send the html message
		self.wfile.write("Hello World !")
		try:
			future = producer.send('querylog', d)
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
