# Import some necessary modules
from kafka import KafkaConsumer
from pymongo import MongoClient
import json


myclient = MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["people"]



    
# connect kafka consumer to desired kafka topic	
consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'])
for msg in consumer:
    record = json.loads(msg.value)
    
    dic = {}
    dic["id"] = record[0]
    dic["addip"] = record[1]
    dic["years"] = record[2]
    dic["moth"] = record[3]
    dic["day"] = record[4]
    dic["heur"] = record[5]
    dic["min"] = record[6]
    dic["sec"] = record[7]
    dic["method"] = record[8]
    dic["status"] = record[9]
    
    x = mycol.insert_one(dic)
    print(dic)
