#### Ερώτημα 4
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone

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

    """Question 4.1) Which edge had the smallest number of vehicles within a predefined time period?"""
    """Ερώτημα 4.1) Ποια ακμή είχε το μικρότερο πλήθος οχημάτων μεταξύ μιας προκαθορισμένης
        χρονικής περιόδου;"""
    min_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vcount", 1).limit(1)
    for doc in min_result:
        print(f"4.1) Link with the smallest number of vehicles: {doc['link']}, Vehicle count: {doc['vcount']}")
    #Same way, only sorting changes
    max_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str}}
    ).sort("vcount", -1).limit(1)
    for doc in max_result:
        print(f"\tFor reference link with biggest number of vehicles: {doc['link']}, Vehicle count: {doc['vcount']}")

    """Question 4.2) Which edge had the highest average speed within a predefined time period?"""
    """Ερώτημα 4.2) Ποια ακμή είχε τη μεγαλύτερη μέση ταχύτητα μεταξύ μιας προκαθορισμένης
    χρονικής περιόδου;"""
    result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str},
         "vspeed": {"$gt": 0}}
    ).sort("vspeed", -1).limit(1)
    for doc in result:
        print(f"4.2) Link with the highest average speed: {doc['link']}, Average speed: {doc['vspeed']}")
    #Same way, only sorting changes
    min_result = proc_collection.find(
        {"time": {"$gte": start_time_str, "$lte": end_time_str},
         "vspeed": {"$gt": 0}}
    ).sort("vspeed", 1).limit(1)
    for doc in min_result:
        print(f"\tFor reference link with the lowest average speed: {doc['link']}, Average speed: {doc['vspeed']}")


    """Question 4.3) What was the longest route within a predefined time period?"""
    """Ερώτημα 4.3) Ποια ήταν η μεγαλύτερη διαδρομή σε μια προκαθορισμένη χρονική περίοδο;"""
    result = raw_collection.aggregate([
        {"$match":{"time": {"$gte": start_time_str, "$lte": end_time_str}}}, # matching time
        {
            "$group": {
                "_id": "$name", # group by vehicle name
                "maxPosition": { "$max": "$position" }, # gets vehicle max position
                "minNonZeroPosition": {
                    "$min": {
                        "$cond": [ # gets vehicle min position
                            { "$gt": ["$position", 0] },
                            "$position",
                            0 # if all are 0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 1,
                "distance": {
                    "$subtract": ["$maxPosition", "$minNonZeroPosition"] # the 2 positions difference is the distance
                }
            }
        },
        {"$sort":{"distance": -1}},
        {"$limit": 1}
    ])
    for doc in result:
        print(f"4.3) Vehicle with the longest route: {doc['_id']}, Distance: {doc['distance']} km")
    #Same way, only sorting changes
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
        print(f"\tFor reference link with the shortest route: {doc['_id']}, Distance: {doc['distance']} km")

if __name__ == "__main__":
    TIMEZONE = timezone.utc
    end_time = datetime.now(TIMEZONE)
    start_time = datetime(2000, 1, 1, 0, 0,  tzinfo=TIMEZONE)
    start_2_minutes= end_time - timedelta(minutes=2)
    print("#" * 5 + f" Last minute results at {end_time} " + "#" * 5)
    query(start_2_minutes, end_time)
    print("#" * 10 + f" Overall results at {end_time} " + "#" * 10)
    query(start_time, end_time)
