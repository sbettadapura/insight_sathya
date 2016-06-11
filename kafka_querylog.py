import pyspark_cassandra
import pyspark_cassandra.streaming

from pyspark_cassandra import CassandraSparkContext

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

import json
import sys
conf = SparkConf() \
    .setAppName("PySpark Cassandra Querylog") \
    .setMaster("spark://"+sys.argv[1]+":7077") \
    .set("spark.cassandra.connection.host", sys.argv[1])

# set up our contexts
sc = CassandraSparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window

kafka_stream = KafkaUtils.createStream(stream, \
                                       "localhost:2181", \
                                       "raw-event-streaming-consumer",
                                        {"querylog":1})
# (None, u'{"site_id": "02559c4f-ec20-4579-b2ca-72922a90d0df", "page": "/something.css"}')
#parsed = kafka_stream.map(lambda v: json.loads(v))
#parsed = kafka_stream.map(lambda (k, v): json.loads(v))
#parsed = kafka_stream.flatMap(lambda (k, v): v)
#parsed = kafka_stream.flatMap(lambda v: v)
#parsed = kafka_stream.map(lambda v: v)
#parsed = kafka_stream.map(lambda (k,v): v)
query = kafka_stream.map(lambda (k,v): json.loads(v))
# aggregate page views by site

print "++++++++++++++++++++++"
query.pprint()
print "++++++++++++++++++++++"
query.saveToCassandra("insight_sathya", "querylog")

stream.start()
stream.awaitTermination()
