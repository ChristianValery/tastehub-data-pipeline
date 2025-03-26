"""
This script reads transaction data from Kafka, processes it in real-time using PySpark,
and writes the results to a PostgreSQL database.

It performs aggregations on product, store sales data, 
and outputs the results to the console for monitoring.
"""


import os
import logging

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col,
                                   from_json,
                                   explode,
                                   window,
                                   sum as spark_sum)
from pyspark.sql.types import (StructType,
                               StructField,
                               StringType,
                               FloatType,
                               IntegerType,
                               ArrayType)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("Kafka-Postgres-Stream")

# Environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
DATABASE_URL = os.getenv('DATABASE_URL')
JDBC_URL = f"jdbc:postgresql://{DATABASE_URL.split('://')[1].split('@')[1]}"
DB_USER = DATABASE_URL.split('://')[1].split(':')[0]
DB_PASSWORD = DATABASE_URL.split('://')[1].split(':')[1].split('@')[0]

logger.info("Starting Spark Session")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TasteHub Real-Time Analysis") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

# Define schema for Kafka messages
item_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("amount", FloatType())
])

transaction_schema = StructType([
    StructField("store_id", IntegerType()),
    StructField("timestamp", StringType()),
    StructField("total_amount", FloatType()),
    StructField("items", ArrayType(item_schema))
])

logger.info("Reading data from Kafka")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "transactions_topic") \
    .option("startingOffsets", "latest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json")

parsed_df = df.select(
    from_json(col("json"), transaction_schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn(
    "timestamp", col("timestamp").cast("timestamp"))

exploded_df = parsed_df.withColumn("item", explode(col("items"))).select(
    col("store_id"),
    col("timestamp"),
    col("total_amount"),
    col("item.product_id"),
    col("item.quantity"),
    col("item.amount")
)
exploded_df.writeStream.format("console").start()

logger.info("Aggregating sales data")

# Sales aggregations
WINDOW_DURATION = "10 minutes"
product_sales_df = exploded_df.groupBy(window("timestamp", WINDOW_DURATION), "product_id") \
    .agg(spark_sum("quantity").alias("total_quantity"), spark_sum("amount").alias("total_sales"))
store_sales_df = exploded_df.groupBy(window("timestamp", WINDOW_DURATION), "store_id") \
    .agg(spark_sum("quantity").alias("total_quantity"), spark_sum("amount").alias("total_sales"))


def write_to_postgres(data, epoch_id, table_name):
    """Write the DataFrame to PostgreSQL."""
    try:
        logger.info("Writing batch %s to %s", epoch_id, table_name)
        data = data.withColumn(
            "processed_at", spark.sql("SELECT CURRENT_TIMESTAMP"))
        data = data.withColumn("window_start", col("window.start")).withColumn(
            "window_end", col("window.end")).drop("window")
        data.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", table_name) \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .mode("append") \
            .save()
        logger.info("Batch %s successfully written to %s",
                    epoch_id, table_name)
    except Exception as e:
        logger.error(
            "Error writing batch %s to %s: %s",
            epoch_id,
            table_name,
            e)


logger.info("Starting streaming queries")

product_query = product_sales_df.writeStream \
    .outputMode("update") \
    .foreachBatch(
        lambda df,
        epoch_id: write_to_postgres(df, epoch_id, "streaming_product_sales")) \
    .start()

store_query = store_sales_df.writeStream \
    .outputMode("update") \
    .foreachBatch(lambda df, epoch_id: write_to_postgres(df, epoch_id, "streaming_store_sales")) \
    .start()

console_query = product_sales_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

logger.info("Waiting for termination")
spark.streams.awaitAnyTermination()
logger.info("Stopping Spark Session")
