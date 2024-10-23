#### Ερώτημα 1.
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

"""Υλοποιήστε στη συνέχεια ένα ξεχωριστό python script με το οποίο κάνουμε query την
MongoDB και απαντούμε στα ακόλουθα ερωτήματα:
1. Ποια ακμή είχε το μικρότερο πλήθος οχημάτων μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;
2. Ποια ακμή είχε τη μεγαλύτερη μέση ταχύτητα μεταξύ μιας προκαθορισμένης
χρονικής περιόδου;
3. Ποια ήταν η μεγαλύτερη διαδρομή σε μια προκαθορισμένη χρονική περίοδο;"""
query = {'speed':{"$gt": 25}}

results = collection.find(query)

for document in results:
    print(document)