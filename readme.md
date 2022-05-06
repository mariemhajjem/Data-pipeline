
Main: data_pipeline.py

ingest from file in transform.py (batch) and kafka topic (stream) in kafka_ingest.py

transform data

store.py

RUN in cmd1 : 
cd data

python iot-devices.py machine1 2

RUN in cmd2 :

spark-submit 

--packages 

org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3,

org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3,

org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 

data_pipeline.py


pip install -U memory_profiler
@profile
def my_func():
    a = [1] * (10 ** 6)
    b = [2] * (2 * 10 ** 7)
    del b
    return a

if __name__ == '__main__':
    my_func()

python -m memory_profiler example.py

mprof run <executable>
mprof plot

