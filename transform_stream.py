from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import mean, max, min, col, current_date, datediff, months_between, lag, desc, when, \
    collect_list, to_timestamp, window
import logging
from pyspark.sql import functions as F, SparkSession

from pyspark.sql.window import Window


class TransformStream:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform_data(self, df_kafka_parsed):
        print("transforming...")
        df_kafka_formatted = df_kafka_parsed.select(
            col("value.ts").alias("timestamp"),
            col("value.machine_name").alias("machine"),
            col("value.temp").alias("temperature")
        )

        # df_kafka_formatted_ts = df_kafka_formatted.withColumn("timestamp",
        #                                                       to_timestamp(df_kafka_formatted.timestamp,
        #                                                                    'yyyy-MM-dd HH:mm:ss'))
        #
        # df_avg_window = df_kafka_formatted_ts.withWatermark("timestamp", "2 minute") \
        #     .groupBy(
        #     window(df_kafka_formatted_ts.timestamp, "1 minute"),
        #     df_kafka_formatted_ts.machine) \
        #     .avg("temperature")

        return df_kafka_formatted
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