#### Ερώτημα 4
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

load_dotenv()
mongo_uri = os.getenv('MONGO_URI')
db_name = os.getenv('MONGO_DB_NAME')
raw_collection_name = os.getenv('MONGO_RAW_DATA_COLLECTION')
proc_collection_name = os.getenv('MONGO_PROC_DATA_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
raw_collection = db[raw_collection_name]
proc_collection = db[proc_collection_name]

end_time = datetime.now()
end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")#[:-3]
start_time = datetime(2000, 1, 1, 0, 0, 0)
start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")


""" Ερώτημα 4.1) Ποια ακμή είχε το μικρότερο πλήθος οχημάτων μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;"""
result = proc_collection.aggregate([
    {"$match": {"time": {"$gte": start_time_str, "$lt": end_time_str}}},
    {"$sort": {"vcount": 1}},
    {"$limit": 1}
])
for doc in result:
    print(f"4.1) Link with the smallest vehicle count: {doc['link']}, Vehicle count: {doc['vcount']}")


""" Ερώτημα 4.2) Ποια ακμή είχε τη μεγαλύτερη μέση ταχύτητα μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;"""
result = proc_collection.aggregate([
    {"$match": {"time": {"$gte": start_time_str, "$lt": end_time_str}}},
    {"$sort": {"vspeed": -1}},
    {"$limit": 1}
])
for doc in result:
    print(f"4.2) Link with the highest average speed: {doc['link']}, Average speed: {doc['vspeed']}")


""" Ερώτημα 4.3) Ποια ήταν η μεγαλύτερη διαδρομή σε μια προκαθορισμένη χρονική περίοδο;"""
result = raw_collection.aggregate([
    {"$match":{"time": {"$gte": start_time_str, "$lt": end_time_str}}},
    {"$sort":{"vspeed": -1}},
    {"$limit": 1}
])
for doc in result:
    print(f"4.3) Link with the longest distance: {doc['link']}, Distance: {doc['position']} km")
