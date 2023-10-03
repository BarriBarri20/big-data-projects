import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
topic_name = 'test'
message = {'name':'toto'}
producer.send(topic_name, message)
producer.flush()
