# imports
from datetime import datetime

from kafka import KafkaProducer  # pip install kafka-python
import numpy as np  # pip install numpy
from sys import argv, exit
from time import time, sleep

#import pymongo
#conn = pymongo.MongoClient('mongodb://localhost')
# db = conn.testDB

#Timeseries collection in mongodb f version 5
#db.create_collection('testColl', timeseries={ 'timeField': 'timestamp' })
# db.command('create', 'testColl', timeseries={ 'timeField': 'timestamp', 'metaField': 'data', 'granularity': 'hours' })

DEVICE_PROFILES = {
    "machine1": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1019.9, 9.5)},
    "machine2": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1012.0, 41.3)},
    "machine3": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1015.9, 11.3)},
}


if len(argv) < 2 or argv[1] not in DEVICE_PROFILES.keys():
    print("please provide a valid device name:")
    for key in DEVICE_PROFILES.keys():
        print(f"  * {key}")
    print(f"\nformat: {argv[0]} DEVICE_NAME")
    exit(1)

machine_name = argv[1]
sleep_time = argv[2]
profile = DEVICE_PROFILES[machine_name]

producer = KafkaProducer(bootstrap_servers="localhost:9095")

count = 0

while True:

    temp = np.random.normal(profile['temp'][0], profile['temp'][1])
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1]), 100))
    pres = np.random.normal(profile['pres'][0], profile['pres'][1])

    # CSV structure
    #msg = f'{count},{time()},{machine_name},{temp},{humd},{pres}'
    import json

    # json string data
    # msg = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "machine_name": machine_name, "temp": temp}
    msg = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "machine_name": machine_name, "temp": temp}
    # convert string to  object
    msg = json.dumps(msg)
    # send to Kafka
    producer.send('raw-data', bytes(msg, encoding='utf8'))

    print(f'sending data to kafka, #{count}')
    sleep(float(sleep_time))
    count += 1
