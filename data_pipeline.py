import logging
import logging.config
from pyspark.sql import SparkSession
import subprocess
import ingest
import kafka_ingest
import store
import transform


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
        kafka_ingestion_process = kafka_ingest.KafkaIngestion(self.spark)
        kafka_ingestion_process.kafka_ingest()
        #
        # ingestion_process = ingest.Ingestion(self.spark)
        # df = ingestion_process.ingest_data()
        #
        # transform_process = transform.Transform(self.spark)
        # transformed_df = transform_process.transform_data(df)
        #
        # store_process = store.Store(self.spark)
        # store_process.store_data(transformed_df)
        return

    def create_spark_session(self):
        self.spark = SparkSession.builder.appName("Data pipeline").master("local[*]").getOrCreate()


if __name__ == '__main__':
    # logging.config.fileConfig("resource/configs/logging.conf")
    logging.info("Starting pipeline")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
