import logging

from pyspark.sql.functions import to_date


class Store:
    def __init__(self, spark):
        self.spark = spark

    def store_data(self, df):
        print("storing...")
        # df.write.option("header","true").csv("transformed_machine_telemetry")
        # Write into HDFS

        try:
            # df.write.csv("hdfs://localhost:9000/process_data/")
            # df.withColumn("date", to_date("datetime")).write.partitionBy("date").format("csv") \
            #    .save("hdfs://localhost:9000/process_data/")
        except Exception as exp:
            logging.error("Erooor when stroing \n"+str(exp))
            raise Exception('HDFS repo already in use')