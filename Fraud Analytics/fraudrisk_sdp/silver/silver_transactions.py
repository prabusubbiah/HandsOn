from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import *
import json

# Schema for the JSON payload in the Kafka 'value' column
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("card_number", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), False),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True),
    ])

@dp.table(
    name="fraudrisk.01_silver.transactions",
    comment="Cleaned Transaction data from Bronze"
)

@dp.expect_or_drop("valid transactions", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid merchant_id", "merchant_id IS NOT NULL")
@dp.expect("valid amount", "amount > 0")
def silver_transactions() -> DataFrame:
    bronze_df = spark.readStream.table("fraudrisk.01_bronze.transactions_stream")

    parsed_stream_df = (
        bronze_df.select(
            F.from_json(F.col("value").cast("string"), schema).alias("data"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
            )
        .select(
            F.col("data.*"),
            F.col("kafka_topic"),
            F.col("kafka_partition"),
            F.col("kafka_offset"),
            F.col("bronze_ingestion_timestamp"),
            F.current_timestamp().alias("silver_ingestion_timestamp")
        )
    )

    return parsed_stream_df
