from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import mean, max, min, col, current_date, datediff, months_between
import logging


class TransformBatch:
    def __init__(self, spark):
        self.spark = spark

    def transform_batch(self, df):
        print("transforming...")
        df.printSchema()

        distinctDF = df.distinct()
        print("Distinct count: " + str(distinctDF.count()))
        df2 = df.dropDuplicates()
        print("Distinct count: " + str(df2.count()))
        df2.show(truncate=False)
