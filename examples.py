from pyspark.sql.functions import to_timestamp, current_timestamp, col
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DoubleType


class Example:
    def __init__(self, spark):
        self.spark = spark

    def examples_data(self):
        print("Examples...")
        print("Select Nested Struct Columns\n")
        data = [
            (("James", None, "Smith"), "OH", "M"),
            (("Anna", "Rose", ""), "NY", "F"),
            (("Julia", "", "Williams"), "OH", "F"),
            (("Maria", "Anne", "Jones"), "NY", "M"),
            (("Jen", "Mary", "Brown"), "NY", "M"),
            (("Mike", "Mary", "Williams"), "OH", "M")
        ]

        schema = StructType([
            StructField('state', StringType(), True),
            StructField('name', StructType([
                StructField('firstname', StringType(), True),
                StructField('middlename', StringType(), True),
                StructField('lastname', StringType(), True)
            ])),
            StructField('state', StringType(), True),
            StructField('gender', StringType(), True)
        ])
        df2 = self.spark.createDataFrame(data=data, schema=schema)
        df2.printSchema()
        df2.show(truncate=False)  # shows all columns
        df2.select("name").show(truncate=False)
        df2.select("name.firstname", "name.lastname").show(truncate=False)
        df2.select("name.*").show(truncate=False)

        dates = [("1", "2019-07-01 12:01:19.111"),
                 ("2", "2019-06-24 12:01:19.222"),
                 ("3", "2019-11-16 16:44:55.406"),
                 ("4", "2019-11-16 16:50:59.406")
                 ]

        df = self.spark.createDataFrame(data=dates, schema=["id", "input_timestamp"])
        df1 = df.withColumn('from_timestamp', to_timestamp(col('from_timestamp'))) \
            .withColumn('end_timestamp', current_timestamp()) \
            .withColumn('DiffInSeconds', col("end_timestamp").cast("long") - col('from_timestamp').cast("long")) \
            .withColumn('DiffInMinutes', round(col('DiffInSeconds') / 60)) \
            .show(truncate=False)
