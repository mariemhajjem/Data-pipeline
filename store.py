import logging

from pyspark.sql.functions import to_date
from pyspark.sql import DataFrame

class Store:
    def __init__(self, spark):
        self.spark = spark

    def store_data(self, df):
        print("storing...")
        # df.write.option("header","true").csv("try_data")
        # Write into HDFS
        # df.write.csv("hdfs://localhost:9000/process_data/")
        # df.withColumn("date", to_date("datetime")).write.partitionBy("date").format("csv") \
        #    .save("hdfs://localhost:9000/process_data/")

        def save(message: DataFrame, epoch_id):

            message.write \
                .format("mongo") \
                .mode("append") \
                .option("database", "industry") \
                .option("collection", "benchmark") \
                .save()
            pass

        try:
            print("storing...")
            query = df.writeStream.outputMode("append").foreachBatch(save).start()
            query.awaitTermination()
        except Exception as exp:
            logging.error("Erooor when storing \n"+str(exp))
            # raise Exception('HDFS repo already in use')


