from kafka import KafkaConsumer
import json
# To consume latest messages and auto-commit offsets
#consumer = KafkaConsumer('my-topic',
                         #group_id='my-group',
                         #bootstrap_servers=['localhost:9092'])
"""
consumer = KafkaConsumer('my-topic')
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
"""
# consume json messages
# consume json messages
consumer = KafkaConsumer('json-topic', value_deserializer=lambda m: json.loads(m.decode('ascii')))
for message in consumer:
    print(message)


