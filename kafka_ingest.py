import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, window, asc, from_csv, unix_timestamp, avg, \
    current_timestamp
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, FloatType
from kafka import KafkaConsumer
import timeit
import time

from mongo import createDocsFromDF
from pymongo import MongoClient


class KafkaIngestion:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def kafka_ingest(self):
        print("Ingesting...")
        # kafka Config
        kafka_broker_hostname = 'localhost'
        kafka_broker_port = '9095'
        kafka_broker = kafka_broker_hostname + ':' + kafka_broker_port
        kafka_topic = 'raw-data'  # data/iot-devices.py simulator

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

        # socket_df: DataFrame = self.spark \
        #     .readStream \
        #     .format("socket") \
        #     .option("host", "localhost") \
        #     .option("port", 9999) \
        #     .load()

        # mqtt_df =(self.spark
        #  .readStream
        #  .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
        ## .option("persistence", "memory")
        #  .load("tcp://{}".format(broker_uri)))

        # convert data from kafka to string
        df_kafka = df.selectExpr("CAST(value AS STRING) as value")

        iot_schema = (StructType()
                      .add("ts", StringType(), True)
                      .add("machine_name", StringType(), True)
                      .add("temp", DoubleType(), True)
                      )

        df_kafka_parsed = df.select(from_json(col("value").cast("string"), iot_schema).alias("value"))

        # formattedStream = df_kafka.select(from_csv(df_kafka.value, iot_schema.simpleString())).alias("value"))
        # df_kafka_parsed = df_kafka.select(from_json(df_kafka.value, iot_schema)).alias("value")

        df_kafka_formatted = df_kafka_parsed.select(
            col("value.ts").alias("timestamp"),
            col("value.machine_name").alias("machine"),
            col("value.temp").alias("temperature")
        )

        diffInSeconds = df_kafka_formatted.withColumn('from_timestamp', to_timestamp(col('timestamp'))) \
            .withColumn('end_timestamp', current_timestamp()) \
            .withColumn('DiffInSeconds', col("end_timestamp").cast("long") - col('from_timestamp').cast("long")) \


        df_kafka_formatted_ts = df_kafka_formatted.withColumn("timestamp",
                                                              to_timestamp(df_kafka_formatted.timestamp,
                                                                           'yyyy-MM-dd HH:mm:ss'))
        df_avg_window = df_kafka_formatted_ts.withWatermark("timestamp", "2 minute").groupBy(
            window(df_kafka_formatted_ts.timestamp, "1 minute"),
            df_kafka_formatted_ts.machine) \
            .avg("temperature")

        df1 = df_kafka_formatted_ts \
            .withColumn('timestamp', unix_timestamp(col('timestamp'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
            .select(col("machine"), col("timestamp"), col("temperature")) \
            .withColumnRenamed("timestamp", "timestamp_update") \
            .withWatermark("timestamp_update", "1 minutes")

        df2 = df_kafka_formatted_ts \
            .withColumn('timestamp', unix_timestamp(col('timestamp'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
            .withWatermark("timestamp", "1 minutes") \
            .groupBy(col("machine"), "timestamp") \
            .agg(avg(col('temperature')).alias("avg_temperature")) \
            .orderBy('timestamp', ascending=False)

        df_avg_window_ts = df_avg_window.select(
            'machine',
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg(temperature)").alias("avg_temperature"),
        ).orderBy('machine', ascending=False)

        def save(message: DataFrame, epoch_id):
            message.write \
                .format("mongo") \
                .mode("append") \
                .option("database", "industry") \
                .option("collection", "iot") \
                .save()
            pass

        # (df_avg_window_ts
        #  .write
        #  .format("mongo")
        #  .mode("append")
        #  .option("ordered", "false")
        #  .option("replaceDocument", "false")
        #  .option("database", client.industry)
        #  .option("collection", "iot")
        #  .save()
        #  )

        query = df_avg_window_ts.writeStream.outputMode("complete").foreachBatch(save).start()

        query.awaitTermination()

        # console = df_avg_window_ts.writeStream.outputMode("complete").format("console").start()
        # console.awaitTermination()
        # console = diffInSeconds.writeStream.outputMode("complete").format("console").start()
        # console.awaitTermination()

        # (df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
        #  .writeStream
        #  .format("kafka")
        #  .outputMode("append")
        #  .option("kafka.bootstrap.servers", kafka_broker)
        #  .option("topic", "josn_data_topic")
        #  .start()
        #  .awaitTermination())
