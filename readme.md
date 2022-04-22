
Run data_pipeline.py
ingest from file in transform.py (batch) and kafka topic (stream) in kafka_ingest.py
transform data
store

cd data
RUN : python iot-devices.py machine1 2

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 data_pipeline.py

