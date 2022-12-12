from kafka import KafkaConsumer
from pymongo import MongoClient
import json

def get_database():
   CONNECTION_STRING = "mongodb://root:rootpassword@mongodb_container:27017"
   client = MongoClient(CONNECTION_STRING)
   return client['spotify']

db = get_database()
commits_collection = db['commits']

consumer = KafkaConsumer('clean-commits', bootstrap_servers=['kafka:9092'], group_id='group1', auto_offset_reset="earliest")
for msg in consumer:
    value = msg.value.decode('utf-8')
    json_object = json.loads(value)
    commits_collection.insert_one(json_object)
   