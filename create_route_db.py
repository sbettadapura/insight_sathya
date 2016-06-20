#!/usr/bin/env python

import sys
import sqlite3
from random import randrange

try:
	num_routes = int(sys.argv[1])
	max_len = int(sys.argv[2])
except:
	print "usage: %s <num_routes> <max_len>" % sys.argv[0]
	exit(-1)

conn = sqlite3.connect('insight_sathya.db')

c = conn.cursor()

c.execute('CREATE TABLE route (route_id int, start int, end int)')

rt_start = 0
for i in range(num_routes):
	rt_len = randrange(max_len/2, max_len)
	route = (i, rt_start, rt_start + rt_len)
	rt_start += rt_len
	c.execute('INSERT INTO route VALUES (?,?,?)', route)

conn.commit()
conn.close()
