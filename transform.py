from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import mean, max, min, col, current_date, datediff, months_between
import logging

class Transform:
    def __init__(self, spark):
        self.spark = spark

    def transform_data(self, df):
        print("transforming...")

        logging.debug("debugging..")
        df.printSchema()
        # df.describe().show()
        # SELECT
        # df.select("machineID", "pressure").show()
        # df.select(df.machineID, df.pressure).show()
        # df.select(df["machineID"], df["pressure"]).show()

        # Select b col() function
        # from pyspark.sql.functions import col
        # df.select(col("machineID"), col("pressure")).show()

        # Select b regex
        # df.select(df.colRegex("`^v.*`")).show()
        # action
        # df.groupBy("machineID").count().show()
        # df.filter("volt > 200").select("datetime", "machineID", "volt").show()
        # df.groupBy("machineID") \
        #     .agg(mean("volt").alias("avg_volt"), mean("volt"), mean("vibration")) \
        #     .show()
        # df.select(
        #     col("datetime"),
        #     current_date().alias("current_date"),
        #     datediff(current_date(), col("datetime")).alias("datediff"),
        #     months_between(current_date(), col("datetime")).alias("monthsdiff")
        # ).show()
        distinctDF = df.distinct()
        print("Distinct count: " + str(distinctDF.count()))
        df1 = df.na.drop()

        return df1
