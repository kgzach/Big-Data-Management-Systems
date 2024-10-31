import os
from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()
try:
    #kafka_broker = os.getenv("REMOTE_BROKER")
    kafka_broker = os.getenv("OFFLINE_BROKER")
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer.send('Test', b'Hello, Kafka!')
    print('Connected to broker')
except Exception as e:
    print('No Brokers Available')
    exit(1)