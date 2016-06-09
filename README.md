# insight_sathya

This is a proto type for getting data from kafka thru a spark-streaming module 
to a cassandra database.

Use myspark.sh to submit run your a program with these compnents.

bash mspark.sh kafka_event_consme.py

Before you can populate a cassandra database, you need to define the schema.
Get into cqlsh. First type "use keyspace demodb;". Then use the verbiage in 
cassandra_db_def.

To produce data run the program fill_kafka.py (this is run indpendently)t is not submitted thru spark-submit).
