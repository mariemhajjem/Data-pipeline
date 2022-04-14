import logging
import logging.config

from pyspark.sql import SparkSession

import ingest
import kafka_ingest
import store
import transform


class Pipeline:
    def __init__(self):
        self.spark = None

    def run_pipeline(self):
        print("Running pipeline")
        # examples_process = examples.Example(self.spark)
        # examples_process.examples_data()
        ingestion_process = kafka_ingest.KafkaIngestion(self.spark)
        ingestion_process.kafka_ingest()

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
        self.spark = SparkSession.builder.appName("Data pipeline").getOrCreate()


if __name__ == '__main__':
    # logging.config.fileConfig("resource/configs/logging.conf")
    logging.info("Starting pipeline")
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()
