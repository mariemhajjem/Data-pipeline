import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, asc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, FloatType
from kafka import KafkaConsumer
import time


class KafkaIngestion:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def kafka_ingest(self):
        print("Ingesting...")
        # kafka Config
        kafka_broker_hostname = 'localhost'
        kafka_broker_port = '9095'
        kafka_broker = kafka_broker_hostname + ':' + kafka_broker_port
        kafka_topic = 'raw-data' # data/iot-devices.py simulator

        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3,' \
                                            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 pyspark-shell '

        # consumer = KafkaConsumer(kafka_topic)

        # Read
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_broker) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # convert data from kafka to string
        df_kafka = df.selectExpr("CAST(value AS STRING) as value")

        iot_schema = (StructType()
                      .add("ts", StringType(), True)
                      .add("machine_name", StringType(), True)
                      .add("temp", DoubleType(), True)
                      )
        df_kafka_parsed = df.select(from_json(col("value").cast("string"), iot_schema).alias("value"))

        # df_kafka_parsed = df_kafka.select(from_json(df_kafka.value, iot_schema)).alias("value")

        df_kafka_formatted = df_kafka_parsed.select(
            col("value.ts").alias("timestamp"),
            col("value.machine_name").alias("machine"),
            col("value.temp").alias("temperature")
        )

        df_kafka_formatted_ts = df_kafka_formatted.withColumn("timestamp",
                                                              to_timestamp(df_kafka_formatted.timestamp,
                                                                           'yyyy-MM-dd HH:mm:ss'))
        df_avg_window = df_kafka_formatted_ts.withWatermark("timestamp", "2 minute").groupBy(
            window(df_kafka_formatted_ts.timestamp, "1 minute"),
            df_kafka_formatted_ts.machine) \
            .avg("temperature")

        df_avg_window_ts = df_avg_window.select(
            'machine',
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg(temperature)").alias("avg_temperature"),
        ).orderBy('machine', asc("window_start"))

        console = df_avg_window_ts.writeStream.outputMode("complete").format("console").start()
        console.awaitTermination()
