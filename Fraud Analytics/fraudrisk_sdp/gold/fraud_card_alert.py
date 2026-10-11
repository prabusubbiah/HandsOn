from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="fraudrisk.03_gold.fruad_card_alert",
    comment="Alert Details, where transaction has been performed with higher than transaction limit"
)

def fraud_card_alert() -> DataFrame:

    transactions=spark.readStream.table("fraudrisk.01_silver.transactions")
    frd_watchlist=spark.readStream.table("fraudrisk.01_silver.fraud_watchlist")

    customer=spark.read.table("fraudrisk.01_silver.customers")

    tran_watermark=transactions.withWatermark("transaction_timestamp","5 minutes")
    fraud_watch_watermark=frd_watchlist.withWatermark("effective_from","5 minutes")

    fruad_detected=(
        tran_watermark.join(
            fraud_watch_watermark,
            tran_watermark.card_number==fraud_watch_watermark.entity_id,
            "inner"
        ).join(
            customer,
            tran_watermark.customer_id==customer.customer_id,
            "left"
        ).select(
            F.concat_ws("-",F.lit("FRAUD"),F.col("transaction_id"),F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias('alert_timestamp'),

            #tranaction_details
            tran_watermark.transaction_id,
            tran_watermark.customer_id,
            customer.email.alias("customer_email"),
            F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
            tran_watermark.card_number,
            tran_watermark.amount,
            tran_watermark.currency,
            tran_watermark.merchant_id,
            tran_watermark.merchant_category,
            tran_watermark.transaction_type,
            tran_watermark.payment_channel,
            tran_watermark.device_id,
            tran_watermark.city.alias("transaction_city"),
            tran_watermark.transaction_timestamp,
            tran_watermark.is_international,
            tran_watermark.status.alias("transaction_status"),

            #watchlist details
            F.col("watchlist_id"),
            F.col("watch_type"),
            F.col("risk_level"),
            F.col("action"),
            F.col("reason_code"),
            F.col("reason_description"),
            F.col("effective_from").alias("watchlist_effective_from"),
            F.col("reported_by"),
            F.col("reported_source"),
            fraud_watch_watermark.city.alias("watchlist_city"),
            fraud_watch_watermark.country.alias("watchlist_country"),
        )
    
        )
    return fruad_detected