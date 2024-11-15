import os
from main import run_script
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()
mongo_uri = os.getenv('MONGO_URI')
db_name = os.getenv('MONGO_DB_NAME')
raw_collection_name = os.getenv('MONGO_RAW_DATA_COLLECTION')
proc_collection_name = os.getenv('MONGO_PROC_DATA_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
raw_collection = db[raw_collection_name]
proc_collection = db[proc_collection_name]

# Test 4.1 & 4.2
new = {
    "name": "Test",
    "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    "link": "Test",
    "vcount": 1_000_000,
    "vspeed": 1_000_000
}
result = proc_collection.insert_one(new)
print(f"Inserted document ID: {result.inserted_id} in collection: {proc_collection_name}")

# Test 4.3
new = {
    "name":"Test",
    "origin":"START",
    "destination":"END",
    "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    "link": "TestLink",
    "position":1_000_000,
    "spacing":1_000,
    "speed":99
}
result = raw_collection.insert_one(new)
print(f"Inserted document ID: {result.inserted_id} in collection: {raw_collection_name}")