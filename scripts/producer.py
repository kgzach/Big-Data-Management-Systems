## 1.4 ##
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import pandas as pd
last_indices = {}

def send_vehicle_data(bootstrap_servers, topic_name, interval, data):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    start_time = datetime.utcnow()
    count = 7200
    cnt = 0
    try:
        while cnt < count:#while True:
            cnt+=1
            current_time = datetime.utcnow()
            elapsed_time = (current_time - start_time).total_seconds()

            for index, row in data.iterrows():
                vehicle_id = row['name']
                cur_index = row['index']
                #if row['t'] <= elapsed_time:
                if row['t'] <= elapsed_time and int(vehicle_id) < 10: # DEBUG ONLY
                    if row['link'] not in ["waiting_at_origin_node", "trip_end"]:
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
                            producer.send(topic_name, value=message)
                            print(f"Sent data: {message}")

                            # Update the last index for the vehicle
                            last_indices[vehicle_id] = cur_index
    except KeyboardInterrupt:
        print("Stopping data transmission.")
    finally:
        #producer.close()
        pass


num_partitions = 5
replication_factor = 1

interval = 5
send_vehicle_data(kafka_broker, topic_name, interval, df)