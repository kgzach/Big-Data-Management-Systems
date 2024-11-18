#### Ερώτημα 1.4 Producer, reads from df
import os
import json
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from datetime import datetime, timedelta

load_dotenv()

kafka_broker = os.getenv('OFFLINE_BROKER')
db_path = os.getenv('DB_PATH')
topic_name=os.getenv('TOPIC_NAME')
try:
    df = pd.read_csv("vehicle_data.csv")
except FileNotFoundError:
    df = pd.read_csv("../vehicle_data.csv")


last_indices = {}

def send_vehicle_data(bootstrapServers, topicName, data):
    producer = KafkaProducer(
        bootstrap_servers=bootstrapServers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    start_time = datetime.now()
    count = 3600 # needed in order to stop the producer
    cnt = 0
    try:
        while cnt < count:
            cnt+=1
            current_time = datetime.now()
            elapsed_time = (current_time - start_time).total_seconds()

            for index, row in data.iterrows():
                vehicle_id = row['name']
                cur_index = row['index']
                # Index will help t
                #if row['t'] <= elapsed_time:
                if row['t'] <= elapsed_time and int(vehicle_id) < 10: # DEBUG ONLY
                    if row['link'] not in ["waiting_at_origin_node", "trip_end"]:
                        #cur_index doesn't allow for the same message to be sent once and not every time.
                        if vehicle_id not in last_indices or last_indices[vehicle_id] < cur_index:
                            message = {
                                "name": row['name'],
                                "origin": row['orig'],
                                "destination": row['dest'],
                                "time": (start_time + timedelta(seconds=row['t'])).isoformat(),
                                "link": row['link'],
                                "position": row['x'],
                                "spacing": row['s'],
                                "speed": row['v']
                            }
                            producer.send(topicName, value=message)
                            print(f"Sent data: {message}")

                            # Update the last index for the vehicle
                            last_indices[vehicle_id] = cur_index
    except KeyboardInterrupt:
        print("Stopping data transmission.")
    finally:
        producer.close()


if not df.empty:
    num_partitions = 1
    replication_factor = 1
    interval = 5
    print("Start sending data")
    send_vehicle_data(kafka_broker, topic_name, df)
else:
    print("DataFrame is empty. Exiting...")
