import os
from dotenv import load_dotenv
from pymongo import MongoClient

def test_mongo_connection():
    load_dotenv()

    # Get MongoDB connection details
    uri = os.getenv('MONGO_URI')
    db_name = os.getenv('MONGO_DB_NAME')
    raw_collection = os.getenv('MONGO_RAW_DATA_COLLECTION')
    processed_collection = os.getenv('MONGO_PROC_DATA_COLLECTION')

    print(f"Testing MongoDB connection with URI: {uri}")

    try:
        client = MongoClient(uri)
        client.server_info()
        print("✓ Successfully connected to MongoDB")

        # Check database
        db = client[db_name]
        print(f"✓ Accessed database: {db_name}")

        # List all collections
        collections = db.list_collection_names()
        print(f"\nExisting collections in {db_name}:")
        for collection in collections:
            print(f"- {collection}")
            # Get collection stats
            stats = db.command("collStats", collection)
            print(f"  Documents: {stats['count']}")

        # Check specific collections
        print(f"\nChecking for required collections:")
        print(f"Raw data collection ({raw_collection}): {'✓' if raw_collection in collections else '✗'}")
        print(f"Processed data collection ({processed_collection}): {'✓' if processed_collection in collections else '✗'}")

    except ConnectionError as e:
        print(f"✗ Connection Error: {str(e)}")
    except Exception as e:
        print(f"✗ Unexpected Error: {str(e)}")

if __name__ == "__main__":
    test_mongo_connection()