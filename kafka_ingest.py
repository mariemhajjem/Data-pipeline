from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


class KafkaIngestion:
    def __init__(self, spark):
        self.spark = spark

    def kafka_ingest(self):
        print("Ingesting...")
        customSchema = StructType([
            StructField("machineID", StringType(), True),
            StructField("pressure", DoubleType(), True)
        ])

        df = self.spark.read.csv("Machine_telemetry.csv", header=True, inferSchema=True)  # schema=customSchema
        # df_load = sparkSession.read.csv('hdfs://localhost:9000/')
        return df
