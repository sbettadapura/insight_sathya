import psycopg2
import os
import datetime
TS_FMT = '%Y-%m-%d %H:%M:%S'
HOST = os.environ['REDSHIFT_HOST']
PORT = 5439 # redshift default
USER = os.environ['REDSHIFT_USER']
PASSWORD = os.environ['REDSHIFT_PASSWD']
DATABASE = os.environ['REDSHIFT_DATABASE']

def db_connection():
	conn = psycopg2.connect(
	host=HOST,
	port=PORT,
	user=USER,
	password=PASSWORD,
	database=DATABASE,
    )

	return conn
#cursor.execute('INSERT INTO %s (day, elapsed_time, net_time, length, average_speed, geometry) VALUES (%s, %s, %s, %s, %s, %s)', (escaped_name, day, time_length, time_length_net, length_km, avg_speed, myLine_ppy))
#cursor.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
#cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)",(100, "abc'def"))

#create_query = ('CREATE TABLE def (route_id integer, start integer, ts timestamp)')
#create_query = ('CREATE TABLE hist_traffic (tod datetime,  route_num integer, location integer, users_total integer, users_affceted integer, duration integer)')

conn = db_connection()
try:
	cursor = conn.cursor()
	#cursor.execute(create_query)
	#cursor.execute("SELECT route_num, location, avg(users_total), avg(users_affceted) from hist_traffic GROUP BY route_num, location, users_total, users_affceted ORDER BY route_num, location")
	"""
	cursor.execute("SELECT sum(users_total), sum(users_affceted) FROM hist_traffic WHERE date(tod) = '2016-06-30'")
	for i in cursor.fetchall():
		print i
	cursor.execute("SELECT AVG(users_total), AVG(users_affceted), AVG(duration) FROM hist_traffic WHERE extract(month from tod) = 7")
	for i in cursor.fetchall():
		print i
	cursor.execute("SELECT sum(users_total), sum(users_affceted) FROM hist_traffic WHERE date(tod) = '2016-06-30'")
	"""
	#cursor.execute("SELECT * from hist_traffic as h1 JOIN (SELECT * FROM hist_traffic as h2  WHERE extract(month from tod) = 7) as h2 ON extract(month from h1.tod) = extract(month from h2.tod)")
	#cursor.execute("SELECT extract(month from h1.tod), h1.users_total from hist_traffic as h1 JOIN (SELECT extract(month from tod), AVG(users_total) FROM hist_traffic GROUP BY extract(month from tod)) as h2 ON extract(month from h1.tod) = extract(month from h2.tod) WHERE extract(month from h2.tod) = 7")
	#cursor.execute("SELECT extract(year from tod), extract(month from tod), extract(day from tod), AVG(users_total), AVG(users_affceted), AVG(duration) from hist_traffic GROUP BY  extract(month from tod)") 
	#cursor.execute("SELECT extract(month from tod), AVG(users_total), AVG(users_affceted), AVG(duration) from hist_traffic GROUP BY  extract(month from tod)") 
	#cursor.execute("SELECT tod, AVG(users_total), AVG(users_affceted), AVG(duration) from hist_traffic GROUP BY  tod") 
	#cursor.execute("SELECT tod, AVG(users_total), AVG(users_affceted), AVG(duration) from hist_traffic GROUP BY  tod") 
	#cursor.execute("CREATE TABLE hist_mmdd as SELECT extract(month from tod)as month, extract(day from tod) as day, users_total, users_affceted as users_affected, duration from hist_traffic")
	#cursor.execute("CREATE TABLE hist_md as SELECT (month + day) as md, users_total, users_affected, duration from hist_mmdd")
	#cursor.execute("SELECT * from hist_md")
	#for i in cursor.fetchall():
		#print i
	#print "+++++++++++++++++++++++++"
	#cursor.execute("SELECT md, AVG(users_total), AVG(users_affected), AVG(duration) from hist_md GROUP BY md")
	#for i in cursor.fetchall():
		#print i
	#cursor.execute("SELECT to_char(tod, 'YYYY-MM-DD HH:MM:SS'), route_num, location, users_total, users_affceted, duration from hist_traffic")
	cursor.execute("CREATE TABLE hist_day_time as SELECT to_char(tod, 'YYYY-MM-DD') as idate, to_char(tod, 'HH:MM:SS') as itime, route_num, location, users_total, users_affceted, duration from hist_traffic")
	#cursor.execute("SELECT * from hist_day_time")
	cursor.execute("SELECT idate, SUM(users_total), SUM(users_affceted), AVG(duration) from hist_day_time GROUP BY idate")
	for i in cursor.fetchall():
		print i
	cursor.execute("SELECT route_num, location, SUM(users_affceted) as affected from hist_day_time GROUP BY route_num, location ORDER BY affected DESC LIMIT 10")
	#cursor.execute("SELECT route_num, location, MAX(users_affceted) as affected from hist_day_time GROUP BY route_num, location LIMIT 10")
	for i in cursor.fetchall():
		print i
finally:
	conn.close()
