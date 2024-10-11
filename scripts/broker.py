import os
#https://toruseo.jp/UXsim/docs/index.html
#      Example code that was given
from uxsim import *
import itertools
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from kafka import KafkaProducer
import pyspark
from dotenv import load_dotenv

load_dotenv()

try:
    #kafka_broker = os.getenv("REMOTE_BROKER")
    kafka_broker = os.getenv("OFFLINE_BROKER")
    producer = KafkaProducer(bootstrap_servers=kafka_broker)
    producer.send('Test', b'Hello, Kafka!')
    print('Conencted to broker')
except Exception as e:
    print('No Brokers Available')

uri = os.getenv('MONGO_URI')
client = MongoClient(uri, server_api=ServerApi('1'))
try:
    db = client['BigData']
    db.create_collection('Test')
except Exception as e:
    print(e)
    print('fail')


seed = None

W = World(
    name="",
    deltan=5,
    tmax=3600, #1 hour simulation
    print_mode=1, save_mode=0, show_mode=1,
    random_seed=seed,
    duo_update_time=600
)
random.seed(seed)

# network definition
"""
    N1  N2  N3  N4
    |   |   |   |
W1--I1--I2--I3--I4-<E1
    |   |   |   |
    v   ^   v   ^
    S1  S2  S3  S4
"""

signal_time = 20
sf_1=1
sf_2=1

I1 = W.addNode("I1", 1, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I2 = W.addNode("I2", 2, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I3 = W.addNode("I3", 3, 0, signal=[signal_time*sf_1,signal_time*sf_2])
I4 = W.addNode("I4", 4, 0, signal=[signal_time*sf_1,signal_time*sf_2])
W1 = W.addNode("W1", 0, 0)
E1 = W.addNode("E1", 5, 0)
N1 = W.addNode("N1", 1, 1)
N2 = W.addNode("N2", 2, 1)
N3 = W.addNode("N3", 3, 1)
N4 = W.addNode("N4", 4, 1)
S1 = W.addNode("S1", 1, -1)
S2 = W.addNode("S2", 2, -1)
S3 = W.addNode("S3", 3, -1)
S4 = W.addNode("S4", 4, -1)

#E <-> W direction: signal group 0
for n1,n2 in [[W1, I1], [I1, I2], [I2, I3], [I3, I4], [I4, E1]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=50, jam_density=0.2, number_of_lanes=3, signal_group=0)

#N -> S direction: signal group 1
for n1,n2 in [[N1, I1], [I1, S1], [N3, I3], [I3, S3]]:
    W.addLink(n1.name+n2.name, n1, n2, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)

#S -> N direction: signal group 2
for n1,n2 in [[N2, I2], [I2, S2], [N4, I4], [I4, S4]]:
    W.addLink(n2.name+n1.name, n2, n1, length=500, free_flow_speed=30, jam_density=0.2, signal_group=1)


# random demand definition every 30 seconds
dt = 30
demand = 2 #average demand for the simulation time
demands = []
for t in range(0, 3600, dt):
    dem = random.uniform(0, demand)
    for n1, n2 in [[N1, S1], [S2, N2], [N3, S3], [S4, N4]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.25)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})
    for n1, n2 in [[E1, W1], [N1, W1], [S2, W1], [N3, W1],[S4, W1]]:
        W.adddemand(n1, n2, t, t+dt, dem*0.75)
        demands.append({"start":n1.name, "dest":n2.name, "times":{"start":t,"end":t+dt}, "demand":dem})

W.exec_simulation()
W.analyzer.print_simple_stats()
W.analyzer.basic_to_pandas()
W.analyzer.link_to_pandas()
W.analyzer.link_traffic_state_to_pandas()#.head(20)
W.analyzer.od_to_pandas()
W.analyzer.vehicles_to_pandas()#.head(20)
#W.analyzer.plot_vehicle_log("0")
#df = W.analyzer.link_to_pandas()
df = W.analyzer.vehicles_to_pandas()
#df['av_demand'] = (df['traffic_volume']+df['vehicles_remain'])/3600
#df['signal_time'] = signal_time
df['index'] = range(len(df))

## 1.3 ##
from kafka.admin import KafkaAdminClient, NewTopic

def create_kafka_topic(bootstrap_servers, topic_name, num_partitions, replication_factor):
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id=os.getenv('CLIENT_ID')
    )

    topic_list = [NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic_name}' created successfully")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    finally:
        admin_client.close()
topic_name=os.getenv('TOPIC_NAME')
create_kafka_topic(bootstrap_servers=kafka_broker, topic_name=topic_name, num_partitions=3, replication_factor=1)

