## 1.5 ##
from kafka import KafkaConsumer
import json

def consume_vehicle_data(bootstrap_servers, topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset="earliest",
        enable_auto_commit=True,)
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt as e:
        print("Stopping consumer.")
    finally:
        #consumer.close()
        pass

consume_vehicle_data(kafka_broker, topic_name)
