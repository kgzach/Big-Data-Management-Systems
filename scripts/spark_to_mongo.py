import os
from dotenv import load_dotenv

load_dotenv()

kafka_broker = os.getenv('KAFKA_BROKER')
db_path = os.getenv('DB_PATH')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_DB_COLLECTION')
topic_name=os.getenv('TOPIC_NAME')

def saveToMongo():
    pass