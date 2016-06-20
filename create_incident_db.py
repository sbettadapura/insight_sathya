#!/usr/bin/env python

import sys
import sqlite3

conn = sqlite3.connect('insight_sathya.db')

c = conn.cursor()

c.execute('CREATE TABLE incident (user_id int, route_id int, location int, incident_type int, ts text)')

conn.commit()
conn.close()
