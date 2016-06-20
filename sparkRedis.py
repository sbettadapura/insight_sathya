from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import redis

POOL = redis.ConnectionPool(host='ec2-52-37-251-31.us-west-2.compute.amazonaws.com', port=6379, db=0)

def sendPartition(iter):
	# ConnectionPool is a static, lazily initialized pool of connections
	#connection = ConnectionPool.get_connection()
	#my_server = redis.StrictRedis(host ='ec2-52-37-251-31.us-west-2.compute.amazonaws.com', port = 6379)
	#print "connection = ", connection
	for record in iter:
		my_server = redis.redis(connection_pool=POOL)
		my_server.set("Word:" + record, 1)
	#ConnectionPool.returnConnection(connection)

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

#Using this context, we can create a DStream that represents 
#streaming data from a TCP source, specified as 
#hostname (e.g. localhost) and port (e.g. 9999).

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("127.0.0.1", 9997)

#This lines DStream represents the stream of data that will be 
#received from the data server. Each record in this 
#DStream is a line of text. Next, we want to split the lines by space into words.

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

#flatMap is a one-to-many DStream operation that 
#creates a new DStream by generating multiple new records 
#from each record in the source DStream. In this case, 
#each line will be split into multiple words and the 
#stream of words is represented as the words DStream. 
#Next, we want to count these words.

# Count each word in each batch
print "words  = ", 
words.pprint()
words.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
