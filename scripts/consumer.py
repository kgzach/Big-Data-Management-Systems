## 1.5 ##
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer


load_dotenv()

kafka_broker = os.getenv('OFFLINE_BROKER')
db_path = os.getenv('DB_PATH')
topic_name=os.getenv('TOPIC_NAME')

def consume_vehicle_data(bootstrap_servers, topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset="earliest",
        enable_auto_commit=True,)
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("Stopping consumer.")
    finally:
        consumer.close()

print("Start consuming")
consume_vehicle_data(kafka_broker, topic_name)


