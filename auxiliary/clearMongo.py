import os
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


raw_collection.delete_many({})
proc_collection.delete_many({})

raw_is_empty = raw_collection.count_documents({}) == 0
proc_is_empty = proc_collection.count_documents({}) == 0

if raw_is_empty and proc_is_empty:
    print("Collections are empty.")
elif not raw_is_empty:
    print("Raw collection is not empty.")
else:
    print("Processed collection is not empty.")