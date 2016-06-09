from sys import path, argv
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
path.append("")

import time
import json
from uuid import uuid4
import random


#from killranalytics.models import PageViews

producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))
# collect metrics for 10 sites
sites = ["02559c4f-ec20-4579-b2ca-72922a90d0df"]  + [str(uuid4()) for x in range(100)]
pages = ["/index.html", "/archive.php", "/whatever.js", "/something.css"]

try:
    per_second = float(argv[1])
except:
    print "Using default of 5 / sec"
    per_second = 5.0

rate = 1 / per_second

print rate

for x in range(1000000):
	d = {'site_id': random.choice(sites), 'page_view':random.choice(pages)}

	print json.dumps(d)
	# produce json messages
	future = producer.send('pageviews', d)
    	time.sleep(rate)

	# Block for 'synchronous' sends
	try:
    		record_metadata = future.get(timeout=10)
	except KafkaError:
		# Decide what to do if produce request failed...
    		log.exception()
    		pass

	# Successful result returns assigned partition and offset
	print (record_metadata.topic)
	print (record_metadata.partition)
	print (record_metadata.offset)
