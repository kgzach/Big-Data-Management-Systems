import os
from dotenv import load_dotenv

load_dotenv()

kafka_broker = os.getenv('KAFKA_BROKER')
db_path = os.getenv('DB_PATH')
topic_name=os.getenv('TOPIC_NAME')