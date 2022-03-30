from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType, BooleanType, ArrayType


class Ingestion:
    def __init__(self, spark):
        self.spark = spark

    def ingest_data(self):
        print("Ingesting...")
        customSchema = StructType([
            StructField("machineID", StringType(), True),
            StructField("pressure", DoubleType(), True)
        ])

        shopFloorDataSchema = StructType([
            StructField('_id', StringType(), True),
            StructField('ofId', StringType(), True),
            StructField('lineId', StringType(), True),
            StructField('lineLabel', StringType(), True),
            StructField('shopFloorType', StringType(), True),
            StructField('dateStart', StringType(), True),
            StructField('dateFinish', StringType(), True),
            StructField('data', ArrayType(StructType([
                StructField('type', StringType()),
                StructField('quantity', IntegerType())
            ]))),
            StructField('_class', StringType(), True),
            StructField('prodStatus', BooleanType(), True)
        ])

        df = self.spark.read.csv("Machine_telemetry.csv", header=True, inferSchema=True)  # schema=customSchema
        # df_load = sparkSession.read.csv('hdfs://localhost:9000/')
        df.printSchema()
        return df
