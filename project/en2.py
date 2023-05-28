from pykafka import KafkaClient
import json
from datetime import datetime
import uuid
import csv
import pandas as pd
import time
import re
import numpy as np

#KAFKA PRODUCER
client = KafkaClient(hosts="localhost:9092")
topic = client.topics['test2']
producer = topic.get_sync_producer()

usersDf = pd.read_csv('data.csv', skiprows=[i for i in range(1,10)])
products_list = usersDf.values.tolist()
#CONSTRUCT MESSAGE AND SEND IT TO KAFKA



for l in products_list:
    data_arr = []
    if(str(l[0])!="" and str(l[1])!="" and str(l[2])!="" and str(l[3])!="" and str(l[5])!=""):
        
        
        w=str(l[4])+str(l[7])+str(l[8])
        data_arr.append(w)
        data_arr.append(int(l[5]))
        
    message = json.dumps(data_arr)
    print(message)
    producer.produce(message.encode('ascii'))
    time.sleep(1)

        
    
    
