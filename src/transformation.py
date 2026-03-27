from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import col, year, month,trim

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

def read_orders(spark, path):
    return spark.read.csv(
        path,
        header=True,
        schema=order_schema
    )

def transform_orders(df):
    return df \
        .filter(col("amount").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("status", trim(col("status"))) \
        .filter(trim(col("status")) == "COMPLETED") \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date")))

if __name__ == "__main__":
    spark = SparkSession.builder.appName("orders_etl").getOrCreate()

    df = read_orders(spark, "orders.csv")
    print("file")
    result = transform_orders(df)
    result.show(5)
    result.write.mode("overwrite").partitionBy("year", "month").parquet("/tmp/routput/orders")
