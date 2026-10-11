from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="fraudrisk.03_gold.transaction_counts_minuts",
    comment="Realtime transaction counts by minutes"
)

def transaction_counts_minutes() -> DataFrame:

    transactions=spark.readStream.table("fraudrisk.01_silver.transactions")


    tran_watermark=transactions.withWatermark("transaction_timestamp","5 minutes")

    transaction_counts_df=tran_watermark.groupBy(
        F.window("transaction_timestamp","1 minute")
        ).agg(
        F.count("*").alias("transaction_count")
        ).select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("Window_end"),
            F.col("transaction_count")
            )
    return transaction_counts_df
