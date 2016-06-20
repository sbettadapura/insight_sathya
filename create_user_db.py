#!/usr/bin/env python

import sys
import sqlite3

conn = sqlite3.connect('insight_sathya.db')

c = conn.cursor()

c.execute('CREATE TABLE user (user_id int, ts text, route_id int, start int, end int)')

conn.commit()
conn.close()
