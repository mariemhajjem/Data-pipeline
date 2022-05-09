import os
from datetime import datetime

from bson import timestamp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp, window, asc, from_csv, unix_timestamp, avg, \
    current_timestamp, lag, substring, date_trunc
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, FloatType
from kafka import KafkaConsumer
import timeit
import time
import threading


class KafkaIngestion:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def kafka_ingest(self):
        print("Ingesting...")
        # kafka Config
        kafka_broker_hostname = 'localhost'
        kafka_broker_port = '9095'
        kafka_broker = kafka_broker_hostname + ':' + kafka_broker_port
        kafka_topic = 'machine1'  # 'topic-1' data/iot-devices.py simulator
        kafka_topics = 'data,topic-1'
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.3,' \
                                            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 pyspark-shell'

        # os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'

        # consumer = KafkaConsumer(kafka_topic)

        # Read
        # df = self.spark.readStream \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", kafka_broker) \
        #     .option("subscribe", kafka_topic) \
        #     .option("startingOffsets", "earliest") \
        #     .load()

        def save(message: DataFrame, epoch_id):
            message.write \
                .format("mongo") \
                .mode("append") \
                .option("database", "industry") \
                .option("collection", "test") \
                .save()
            pass

        temp_schema = (StructType()
                       .add("ts", TimestampType(), True)
                       .add("machine_name", StringType(), True)
                       .add("temp", DoubleType(), True)
                       )

        humd_schema = (StructType()
                       .add("ts", TimestampType(), True)
                       .add("machine_name", StringType(), True)
                       .add("humd", DoubleType(), True)
                       )

        pres_schema = (StructType()
                       .add("ts", TimestampType(), True)
                       .add("machine_name", StringType(), True)
                       .add("pres", DoubleType(), True)
                       )

        def read(topic, schema: StructType):
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_broker) \
                .option("subscribePattern", topic) \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)")

            df_formatted = df.select(from_json(col("value").cast("string"), schema).alias("value"))
            if schema == temp_schema:
                print(temp_schema)
                df_kafka_formatted = df_formatted.select(
                    col("value.ts").alias("timestamp"),
                    col("value.machine_name").alias("machine"),
                    col("value.temp").alias("temperature")
                )
            elif schema == humd_schema:
                df_kafka_formatted = df_formatted.select(
                    col("value.ts").alias("timestamp"),
                    col("value.machine_name").alias("machine"),
                    col("value.humd").alias("humidity")
                )
            else:
                df_kafka_formatted = df_formatted.select(
                    col("value.ts").alias("timestamp"),
                    col("value.machine_name").alias("machine"),
                    col("value.pres").alias("pression")
                )

            query = df_kafka_formatted.writeStream.outputMode("append").foreachBatch(save).start()
            query.awaitTermination()

        # my_topics = ["topic-machine1", "topic-machine2", "topic-machine3"]
        my_topics = {"topic-machine1": temp_schema, "topic-machine2": humd_schema, "topic-machine3": pres_schema}

        def run_with_threads():
            threads = []
            for key, value in my_topics.items():
                # `args` is a tuple specifying the positional arguments for the
                # target function, which will be run in an independent thread.
                thread = threading.Thread(target=read, args=(key, value))
                threads.append(thread)
                thread.start()

            for thread in threads:
            # With `join`, we wait until the thread terminates, either normally
            # or through an unhandled exception.
                thread.join()

        run_with_threads()

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
        # df_kafka = df.selectExpr("CAST(value AS STRING) as value")
        #
        # iot_schema = (StructType()
        #               .add("ts", TimestampType(), True)
        #               .add("machine_name", StringType(), True)
        #               .add("temp", DoubleType(), True)
        #               )
        #
        # df_kafka_parsed = df_kafka.select(from_json(col("value").cast("string"), iot_schema).alias("value"))
        # return df_kafka_parsed

        # formattedStream = df_kafka.select(from_csv(df_kafka.value, iot_schema.simpleString()).alias("value"))
        # formattedStream = df_kafka.select(from_csv(df_kafka.value, iot_schema.simpleString())).alias("value"))
        # df_kafka_parsed = df_kafka.select(from_json(df_kafka.value, iot_schema)).alias("value")

        # df_kafka_formatted = df_kafka_parsed.select(
        #     col("value.ts").alias("timestamp"),
        #     col("value.machine_name").alias("machine"),
        #     col("value.temp").alias("temperature")
        # )

        # diffInSeconds = df_kafka_formatted \
        #     .withColumn('DiffInSeconds', current_timestamp().cast("long") - to_timestamp(col('timestamp')).cast("long")) \
        #     .withColumn('DiffInMinutes', col('DiffInSeconds') / 60)

        # diffInSeconds = df_kafka_formatted \
        #     .withColumn('current_ts', current_timestamp().cast("String"))
        #
        # timeFmt = "yyyy-MM-dd' 'HH:mm:ss.SSS"
        #
        # dfSeconds = diffInSeconds \
        #     .withColumn('max_milli',
        #                 unix_timestamp('current_ts', format=timeFmt) + substring('current_ts', -3, 3).cast('float') / 1000) \
        #     .withColumn('min_milli',
        #                 unix_timestamp('timestamp', format=timeFmt) + substring('timestamp', -3, 3).cast('float') / 1000) \
        #     .withColumn('ingestion_ms', (col('max_milli') - col('min_milli')).cast('float') * 1000)
        # dff= dfSeconds.select(col("timestamp"), col("machine"), col("temperature"), col('ingestion_ms'))
        # diffInSeconds = df_kafka_formatted.select(date_trunc('MILLISECOND', to_timestamp(current_timestamp() - to_timestamp(col('timestamp')))))

        # df1 = df_kafka_formatted.withColumn("unix_timestamp",
        #                                     unix_timestamp(df_kafka_formatted.timestamp,
        #                                                    'dd-MMM-yyyy HH:mm:ss.SSS z') + substring(
        #                                         df_kafka_formatted.timestamp, -7, 3).cast(
        #                                         'float') / 1000)

        # console = dff.writeStream.outputMode("append").format("console").start()
        # console.awaitTermination()
        # order = df_kafka_formatted.order

        # df_kafka_formatted_ts = df_kafka_formatted.withColumn("timestamp",
        #                                                       to_timestamp(df_kafka_formatted.timestamp,
        #                                                                    'yyyy-MM-dd HH:mm:ss'))
        #
        # df_avg_window = df_kafka_formatted_ts.withWatermark("timestamp", "2 minute") \
        #     .groupBy(
        #     window(df_kafka_formatted_ts.timestamp, "1 minute"),
        #     df_kafka_formatted_ts.machine) \
        #     .avg("temperature")

        # console = df_avg_window_ts.writeStream.outputMode("complete").format("console").start()
        # console.awaitTermination()

        # df1 = df_kafka_formatted_ts \
        #     .withColumn('timestamp', unix_timestamp(col('timestamp'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
        #     .select(col("machine"), col("timestamp"), col("temperature")) \
        #     .withColumnRenamed("timestamp", "timestamp_update") \
        #     .withWatermark("timestamp_update", "1 minutes")
        #
        # df2 = df_kafka_formatted_ts \
        #     .withColumn('timestamp', unix_timestamp(col('timestamp'), "MM/dd/yyyy hh:mm:ss aa").cast(TimestampType())) \
        #     .withWatermark("timestamp", "1 minutes") \
        #     .groupBy(col("machine"), "timestamp") \
        #     .agg(avg(col('temperature')).alias("avg_temperature")) \
        #     .orderBy('timestamp', ascending=False)

        # df_avg_window_ts = df_avg_window.select(
        #     'machine',
        #     col("window.start").alias("timestamp"),
        #     col("window.start").alias("window_start"),
        #     col("window.end").alias("window_end"),
        #     col("avg(temperature)").alias("avg_temperature"),
        # ).orderBy('timestamp', ascending=False)

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

        # query = df_avg_window_ts.writeStream.outputMode("complete").foreachBatch(save).start()
        # query.awaitTermination()

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
