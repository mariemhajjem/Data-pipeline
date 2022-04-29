from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, avg, min, corr, max
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType, FloatType


class FileIngest:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def file_ingest(self):
        print("Ingesting from file...")

        file_schema = (StructType()
                       .add("datetime", StringType(), True)
                       .add("machineID", StringType(), True)
                       .add("volt", DoubleType(), True)
                       .add("rotate", DoubleType(), True)
                       .add("pressure", DoubleType(), True)
                       .add("vibration", DoubleType(), True)
                       )
        df_csv = (self.spark.readStream
                  .option("sep", ",")
                  .option("header", None)
                  .schema(file_schema).csv("files"))

        df_csv.createOrReplaceTempView("machines")

        # avg_telemetries = self.spark.sql(
        #     "select machineID,avg(volt) as avg_volt, avg(rotate) "
        #     "as avg_rotate,avg(pressure) as avg_pressure, avg(vibration) as avg_vibration "
        #     "from machines group by machineID")

        avg_telemetries = df_csv \
            .select(col("machineID"), col("volt"), col("rotate"), col("pressure"), col("vibration")) \
            .groupBy(col("machineID")) \
            .agg(avg(col('volt')).alias("avg_volt"), min(col('volt')).alias("min_volt"),
                 max(col('volt')).alias("max_volt"),
                 avg(col('rotate')).alias("avg_rotate"), min(col('rotate')).alias("min_rotate"),
                 max(col('rotate')).alias("max_rotate"),
                 avg(col('pressure')).alias("avg_pressure"), min(col('pressure')).alias("min_pressure"),
                 max(col('pressure')).alias("max_pressure"),
                 avg(col('vibration')).alias("avg_vibration"), min(col('vibration')).alias("min_vibration"),
                 max(col('vibration')).alias("max_vibration"),
                 corr(col('volt'), col('rotate')).alias("corr_volt_rotate"),
                 corr(col('volt'), col('vibration')).alias("corr_volt_vibration"),
                 corr(col('volt'), col('pressure')).alias("corr_volt_pressure"),
                 corr(col('rotate'), col('vibration')).alias("corr_rotate_vibration"),
                 corr(col('rotate'), col('pressure')).alias("corr_rotate_pressure"),
                 corr(col('pressure'), col('vibration')).alias("corr_pressure_vibration"),
                 )

        df1 = avg_telemetries.na.drop()

        def save(message: DataFrame, epoch_id):
            message.write \
                .format("mongo") \
                .mode("append") \
                .option("database", "industry") \
                .option("collection", "transformed_data") \
                .save()
            pass

        query = df1.writeStream.outputMode("complete").foreachBatch(save).start()
        #
        query.awaitTermination()

        # console = df1.writeStream.outputMode("complete").format("console").start()
        # console.awaitTermination()
