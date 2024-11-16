#### Ερώτημα 4
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta

def query(start_time_str, end_time_str) -> None:

    load_dotenv()
    mongo_uri = os.getenv('MONGO_URI')
    db_name = os.getenv('MONGO_DB_NAME')
    raw_collection_name = os.getenv('MONGO_RAW_DATA_COLLECTION')
    proc_collection_name = os.getenv('MONGO_PROC_DATA_COLLECTION')

    client = MongoClient(mongo_uri)
    db = client[db_name]
    raw_collection = db[raw_collection_name]
    proc_collection = db[proc_collection_name]


    """ Ερώτημα 4.1) Ποια ακμή είχε το μικρότερο πλήθος οχημάτων μεταξύ μιας προκαθορισμένης
        χρονικής περιόδου;"""
    min_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vcount", 1).limit(1)
    for doc in min_result:
        print(f"4.1) Link with the smallest vehicle count: {doc['link']}, Vehicle count: {doc['vcount']}")

    max_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vcount", -1).limit(1)
    for doc in max_result:
        print(f"\tFor reference link with biggest count: {doc['link']}, Vehicle count: {doc['vcount']}")


    """ Ερώτημα 4.2) Ποια ακμή είχε τη μεγαλύτερη μέση ταχύτητα μεταξύ μιας προκαθορισμένης
    χρονικής περιόδου;"""
    result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vspeed", -1).limit(1)
    for doc in result:
        print(f"4.2) Link with the highest average speed: {doc['link']}, Average speed: {doc['vspeed']}")

    min_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vspeed", 1).limit(1)
    for doc in min_result:
        print(f"\tFor reference link with the lowest average speed: {doc['link']}, Average speed: {doc['vspeed']}")


    """ Ερώτημα 4.3) Ποια ήταν η μεγαλύτερη διαδρομή σε μια προκαθορισμένη χρονική περίοδο;"""
    result = raw_collection.aggregate([
        {"$match":{"time": {"$gte": start_time_str, "$lte": end_time_str}}},
        {
            "$group": {
                "_id": "$name",
                "maxPosition": { "$max": "$position" },
                "minNonZeroPosition": {
                    "$min": {
                        "$cond": [
                            { "$gt": ["$position", 0] },
                            "$position",
                            0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "distance": {
                    "$subtract": ["$maxPosition", "$minNonZeroPosition"]
                }
            }
        },
        {"$sort":{"distance": -1}},
        {"$limit": 1}
    ])
    for doc in result:
        print(f"4.3) Vehicle with the longest distance: {doc['_id']}, Distance: {doc['distance']} km")

    min_result = raw_collection.aggregate([
        {"$match":{"time": {"$gte": start_time_str, "$lte": end_time_str}}},
        {
            "$group": {
                "_id": "$name",
                "maxPosition": { "$max": "$position" },
                "minNonZeroPosition": {
                    "$min": {
                        "$cond": [
                            { "$gt": ["$position", 0] },
                            "$position",
                            0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "distance": {
                    "$subtract": ["$maxPosition", "$minNonZeroPosition"]
                }
            }
        },
        {"$sort":{"distance": 1}},
        {"$limit": 1}
    ])
    for doc in min_result:
        print(f"\tFor reference link with the shortest distance: {doc['_id']}, Distance: {doc['distance']} km")

if __name__ == "__main__":
    end_time = datetime.now().strftime("%Y-%m-%dT%H:%M")
    start_time = datetime(2000, 1, 1, 0, 0).strftime("%Y-%m-%dT%H:%M")

    start_1_minute=(datetime.now() - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")

    query(start_1_minute, end_time)
    print("#" * 5 + f"Overall stats at {end_time}" + "#" * 5)
    query(start_time, end_time)
