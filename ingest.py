from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, \
    TimestampType


class Ingestion:
    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting...")

        shopFloorDataSchema = StructType([
            StructField('_id', StringType(), True),
            StructField('ofId', StringType(), True),
            StructField('lineId', IntegerType(), True),
            StructField('lineLabel', StringType(), True),
            StructField('shopFloorType', StringType(), True),
            StructField('dateStart', TimestampType(), True),
            StructField('dateFinish', TimestampType(), True),
            StructField('data.quantity', IntegerType(), True),
            StructField('data.type', StringType(), True),
            StructField('_class', StringType(), True),
            StructField('prodStatus', BooleanType(), True)
        ])

        df = self.spark.read.csv("data/shopFloorData.csv", header=True, inferSchema=True, sep=",")  # schema=shopFloorDataSchema
        # df_load = sparkSession.read.csv('hdfs://localhost:9000/')

        # print("Count: " + str(df.count()))
        # distinctDF = df.distinct()
        # print("Distinct count: " + str(distinctDF.count()))
        return df
