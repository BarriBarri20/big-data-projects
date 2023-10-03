from kafka import KafkaConsumer
topic_name = 'urgent_data'
consumer = KafkaConsumer(topic_name, group_id='new-consumer-group-topic1', auto_offset_reset= "earliest",bootstrap_servers= 'localhost:9092')
for msg in consumer:
	print("Nurse: urgent data")
	print(msg)

