#!/usr/bin/env python

import sys
import sqlite3

conn = sqlite3.connect('insight_sathya.db')

c = conn.cursor()

for i in c.execute('SELECT * FROM incident'):
	print i

conn.close()
