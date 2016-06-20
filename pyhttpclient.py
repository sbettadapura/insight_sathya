import httplib
import time
while True:
	conn = httplib.HTTPConnection("localhost:8180")
	conn.request("GET", "/")
	r1 = conn.getresponse()
	#time.sleep(0.05)
	#print(r1.status, r1.reason)
