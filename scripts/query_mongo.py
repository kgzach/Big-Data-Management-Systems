#### Ερώτημα 4
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta

load_dotenv()
mongo_uri = os.getenv('MONGO_URI')
db_name = os.getenv('MONGO_DB_NAME')
raw_collection_name = os.getenv('MONGO_RAW_DATA_COLLECTION')
proc_collection_name = os.getenv('MONGO_PROC_DATA_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
raw_collection = db[raw_collection_name]
proc_collection = db[proc_collection_name]

#start_time = datetime(2024, 10, 27, 1, 0, 0)
#end_time = datetime(2024, 10, 29, 23, 0, 0)
end_time = datetime.now()
#start_time = end_time - timedelta(seconds=59)
start_time = datetime.combine(end_time.date(), datetime.min.time())
print(f"End: {end_time}, Start: {start_time}")
start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


""" Ερώτημα 4.1) Ποια ακμή είχε το μικρότερο πλήθος οχημάτων μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;"""
result = proc_collection.aggregate([
    {"$match": {"time": {"$gte": start_time_str, "$lt": end_time_str}}},
    #{"$group": {"_id": "$link", "vehicle_count": {"$sum": "$vcount"}}},
    #{"$sort": {"vehicle_count": -1}},
    {"$sort": {"vcount": -1}},
    {"$limit": 1}
])
for doc in result:
    #print(f"4.1) Link with the smallest vehicle count: {doc['link']}, Vehicle count: {doc['vehicle_count']}")
    print(f"4.1) Link with the smallest vehicle count: {doc['link']}, Vehicle count: {doc['vcount']}")

""" Ερώτημα 4.2) Ποια ακμή είχε τη μεγαλύτερη μέση ταχύτητα μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;"""
result = proc_collection.aggregate([
    {"$match": {"time": {"$gte": start_time_str, "$lt": end_time_str}}},
    {"$sort": {"vspeed": 1}},
    {"$limit": 1}
])
for doc in result:
    print(f"4.2) Link with the highest average speed: {doc['link']}, Average speed: {doc['vspeed']}")

""" Ερώτημα 4.3) Ποια ήταν η μεγαλύτερη διαδρομή σε μια προκαθορισμένη χρονική περίοδο;"""
result = raw_collection.find(
        {"time": {"$gte": start_time_str, "$lt": end_time_str}}
    ).sort("position", -1).limit(1)
for doc in result:
    print(f"4.3) Link with the longest distance: {doc['link']}, Distance: {doc['position']} km")

