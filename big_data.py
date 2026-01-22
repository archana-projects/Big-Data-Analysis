from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count

def run_analysis():
    spark = SparkSession.builder \
        .appName(" Big Data Analysis") \
        .getOrCreate()

    df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

    df = df.withColumn("TotalAmount", df.Quantity * df.Price)

    category_sales = df.groupBy("Category") \
        .agg(sum("TotalAmount").alias("Total Sales")) \
        .toPandas()

    avg_order = df.select(avg("TotalAmount").alias("Average Order Value")).toPandas()

    region_orders = df.groupBy("Region") \
        .agg(count("*").alias("Total Orders")) \
        .toPandas()

    spark.stop()

    return category_sales, avg_order, region_orders

