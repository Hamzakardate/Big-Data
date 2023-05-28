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
topic = client.topics['test']
producer = topic.get_sync_producer()

usersDf = pd.read_csv('data.csv', skiprows=[i for i in range(1,10)])
products_list = usersDf.values.tolist()
#CONSTRUCT MESSAGE AND SEND IT TO KAFKA



for l in products_list:
    data_arr = []
    if(str(l[0])!="" and str(l[1])!="" and str(l[2])!="" and str(l[3])!="" and str(l[5])!=""):
        data_arr.append(int(l[0]))
        data_arr.append(str(l[1]))
        datetime_str = str(l[2])
        x = datetime_str.split(" ")
        xx=x[0].split("-")
        y=x[1].split("+")
        yy=y[0].split(":")
        data_arr.append(int(xx[0]))
        data_arr.append(int(xx[1]))
        data_arr.append(int(xx[2]))
        data_arr.append(int(yy[0]))
        data_arr.append(int(yy[1]))
        data_arr.append(int(yy[2]))
        data_arr.append(str(l[3]))
        data_arr.append(int(l[5]))
        
    message = json.dumps(data_arr)
    print(message)
    producer.produce(message.encode('ascii'))
    time.sleep(1)

        
    
    
