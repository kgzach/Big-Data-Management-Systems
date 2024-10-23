import os
from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()
mongo_uri = os.getenv('MONGO_URI')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_DB_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

query = {'speed':{"$gt": 25}}

results = collection.find(query)

for document in results:
    print(document)