import json
import time
from datetime import datetime
import numpy as np
from kafka import KafkaProducer
import sys
producer_timings = {}

# kafka Config
kafka_broker_hostname = 'localhost'
kafka_broker_port = '9095'
bootstrap_servers = kafka_broker_hostname + ':' + kafka_broker_port
topic = 'machine1'

DEVICE_PROFILES = {
    "machine1": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1019.9, 9.5)},
    "machine2": {'temp': (49.5, 19.3), 'humd': (33.0, 13.9), 'pres': (1012.0, 41.3)},
    "machine3": {'temp': (63.9, 11.7), 'humd': (62.8, 21.8), 'pres': (1015.9, 11.3)},
}

machine_name = "machine1"
profile = DEVICE_PROFILES[machine_name]


def python_kafka_producer_performance(topic=topic):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    print("\n>>> Connect Kafka in {}".format(bootstrap_servers))
    producer_start = time.time()
    temp = np.random.normal(profile['temp'][0], profile['temp'][1])

    msg = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "machine_name": machine_name, "temp": temp}
    # convert string to  object
    msg = json.dumps(msg)

    # msg_size = sys.getsizeof(msg)
    # print(msg_size)

    for i in range(1000000):
        temp = np.random.normal(profile['temp'][0], profile['temp'][1])
        msg = {"ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "machine_name": machine_name, "temp": temp}
        # convert string to  object
        msg = json.dumps(msg)
        producer.send(topic, bytes(msg, encoding='utf8'))

    producer.flush()  # clear all local buffers and produce pending messages

    return time.time() - producer_start


def calculate_throughput(timing, n_messages=1000000, msg_size=140):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024 * 1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))


producer_timings['python_kafka_producer'] = python_kafka_producer_performance()
calculate_throughput(producer_timings['python_kafka_producer'])


