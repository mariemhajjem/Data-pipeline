import logging
import logging.config
from time import time

from pyspark.sql import SparkSession
import subprocess
import ingest
import ingest_csv
import kafka_ingest
import store
import transform
import transform_stream

class Pipeline:
    def __init__(self):
        self.spark = None

    def run_pipeline(self):
        print("Running pipeline")
        # Run scripts : zookeeper and kafka server
        # filepath="scripts\start-servers.bat"
        # filepath2 = "scripts\kafka-server.bat"
        # s1 = subprocess.Popen(filepath, shell=True, stdout=subprocess.PIPE)
        # stdout, stderr = s1.communicate()
        # print(s1.returncode)  # is 0 if success
        # s2 = subprocess.Popen(filepath2, shell=True, stdout=subprocess.PIPE)
        # stdout, stderr = s2.communicate()
        # print(s2.returncode)

        # examples_process = examples.Example(self.spark)
        # examples_process.examples_data()
        ############## Pipeline ###################
        # csv_ingestion_process = ingest_csv.FileIngest(self.spark)
        # csv_ingestion_process.file_ingest()
        start_job = time()
        kafka_ingestion_process = kafka_ingest.KafkaIngestion(self.spark)
        kafka_ingestion_process.kafka_ingest()
        ingestion_process_duration = time() - start_job
        print(f"ingestion process finished in {ingestion_process_duration:.2f} seconds.")

        #
        # ingestion_process = ingest.Ingestion(self.spark)
        # df = ingestion_process.ingest_data()
        #
        # start_job = time()
        # # transform_process = transform.Transform(self.spark)
        # # transformed_df = transform_process.transform_data(df)
        # transform_process_duration = time() - start_job
        # print(f"transform process finished in {transform_process_duration:.2f} seconds.")
        #
        # start_job = time()
        # transform_process = transform_stream.TransformStream(self.spark)
        # transformed_df = transform_process.transform_data(df)
        # t_stream_process_duration = time() - start_job
        # print(f"transform stream process finished in {t_stream_process_duration:.2f} seconds.")
        #
        # start_job = time()
        # store_process = store.Store(self.spark)
        # store_process.store_data(transformed_df)
        # storing_process_duration = time() - start_job
        # print(f"process finished in {storing_process_duration:.2f} seconds.")

        # print(f"total time of process finished in {storing_process_duration+transform_process_duration+ingestion_process_duration:.2f} seconds.")

        return

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("Data pipeline") \
            .config("spark.mongodb.input.uri", "mongodb://localhost:27017") \
            .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3") \
            .config("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.0") \
            .master("local[*]")\
            .getOrCreate()


if __name__ == '__main__':
    # logging.config.fileConfig("resource/configs/logging.conf")
    logging.info("Starting pipeline")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
