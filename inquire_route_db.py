#!/usr/bin/env python

import sqlite3

conn = sqlite3.connect('insight_sathya.db')

c = conn.cursor()

for row in c.execute('SELECT * from route'):
	print row
