from kafka import KafkaConsumer
import json
import sys
import sqlite3

REQ_TYPE_REGISTER, REQ_TYPE_SIGNOFF, REQ_TYPE_QUERY, REQ_TYPE_UPDATE = range(4)

SUB_REQ_TYPE_UPDATE_ACCIDENT, SUB_REQ_TYPE_UPDATE_OTHER, SUB_REQ_TYPE_UPDATE_CLEAR = range(3)


def create_db_recs(db_cursor, user_data):
	#print "user_data = "
	#print user_data
	req_type = user_data['req_type']
	print "req_type = ", req_type
	if req_type == REQ_TYPE_REGISTER:
		db_user_create(db_cursor, user_data)
	elif req_type == REQ_TYPE_SIGNOFF:
			db_user_signoff(db_cursor, user_data)
	elif req_type == REQ_TYPE_QUERY:
			db_query_create(db_cursor, user_data)
	elif req_type == REQ_TYPE_UPDATE:
		sub_req_type = user_data["sub_req_type"]
		if sub_req_type == SUB_REQ_UPDATE_CLEAR:
			db_incident_update(db_cursor, user_data)
		else:
			db_incident_clear(db_cursor, user_data)


def get_num_users(db_cursor):
	return db_cursor.execute('SELECT count(*) from user').fetchone()[0]

def notify_user():
	pass

def db_user_signoff(db_cursor, parsed):
	user_id = parsed['user_id']
	db_cursor.execute("DELETE FROM user where user_id = ?", user_id)

def db_user_create(db_cursor, parsed):
	user_id = get_num_users(db_cursor)
	route_id = parsed['route_id']
	ts = parsed['ts']
	start = parsed['start']
	end = parsed['end']
	db_cursor.execute("INSERT INTO user VALUES (?, ?, ?, ?, ?)", (user_id,ts,route_id,start,end))

def db_user_query(db_cursor, parsed):
	route_id = parsed['route_id']
	start = parsed['start']
	end = parsed['end']
	notify_user_incidents = 0
	for incident in db_cursor.execute("SELECT FROM incident where route_id = ?" , (route_id,)):
		location = incident[2]	
		if start <= location and location <= end:
			notify_user_incidents += 1
	if notify_user_incidents != 0:
		notify_user()

def db_incident_update(db_cursor, parsed):
	user_id = parsed['user_id']
	route_id = parsed['route_id']
	location = parsed['location']
	sub_req_type = parsed['sub_req_type']
	ts = parsed['ts']
	db_cursor.execute("INSERT INTO incident VALUES (?, ?, ?, ?, >)", (user_id, route_id, location, sub_req_type, ts))

def db_incident_clear(db_cursor, parsed):
	route_id = parsed['route_id']
	location = parsed['location']
	db_cursor.execute("DELETE FROM incident where route_id = ? and location = ?", route_id, location)


#set up sqlite objects
db_conn = sqlite3.connect('insight_sathya.db')
db_cursor = db_conn.cursor()

# consume json messages
consumer = KafkaConsumer('user_traffic', value_deserializer=lambda m: json.loads(m.decode('ascii')))
for message in consumer:
	print message.value
	create_db_recs(db_cursor, message.value)
	db_conn.commit()

db_conn.close()
