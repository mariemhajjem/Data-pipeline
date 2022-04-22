from pyspark.sql import functions as F, SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, \
    TimestampType


class Ingestion:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting...")
        #
        # shopFloorDataSchema = StructType([
        #     StructField('_id', StringType(), True),
        #     StructField('ofId', StringType(), True),
        #     StructField('lineId', IntegerType(), True),
        #     StructField('lineLabel', StringType(), True),
        #     StructField('shopFloorType', StringType(), True),
        #     StructField('dateStart', TimestampType(), True),
        #     StructField('dateFinish', TimestampType(), True),
        #     StructField('data.quantity', IntegerType(), True),
        #     StructField('data.type', StringType(), True),
        #     StructField('_class', StringType(), True),
        #     StructField('prodStatus', BooleanType(), True)
        # ])
        #
        # df = self.spark.read.csv("data/shopFloorData.csv", header=True, inferSchema=True, sep=",")  # schema=shopFloorDataSchema
        # df_load = sparkSession.read.csv('hdfs://localhost:9000/')

        df = self.spark.read.csv("data/Data_X.csv", header=True, inferSchema=True, sep=",")
        df.head()
        df.printSchema()

        df1 = df.withColumn('flag', F.when(df["T_data_1_1"] < 0, 0).otherwise(1)). \
            filter('flag == "1"'). \
            drop("flag")

        df1.describe().show()

        df1.select(df.T_data_1_1.between(220, 282)).show()
        df1.select("T_data_1_1",  # Show T_data_1_1 and 0 or 1 depending on volt >30
                  F.when(df.T_data_1_1 > 250, 1) \
                  .otherwise(0))

        # print("Count: " + str(df.count()))
        # distinctDF = df.distinct()
        # print("Distinct count: " + str(distinctDF.count()))
        return df
