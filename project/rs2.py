# Import some necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json


myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["customers"]



    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('test2',bootstrap_servers=['localhost:9092'])
for msg in consumer:
    record = json.loads(msg.value)
    
    dic = {}
    dic["text"] = record[0]
    dic["method"] = record[1]
    
    
    x = mycol.insert_one(dic)
    print(dic)
