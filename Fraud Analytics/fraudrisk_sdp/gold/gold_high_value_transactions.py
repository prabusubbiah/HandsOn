from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="fraudrisk.03_gold.high_value_transactions",
    comment="Alerts for High value transactions over configured limit"
)

def high_value_transactions() -> DataFrame:

    transactions = spark.readStream.table("fraudrisk.01_silver.transactions")
    customers=spark.read.table("fraudrisk.01_silver.customers")

    tran_cust_df=(transactions.join(customers,transactions.customer_id==customers.customer_id,"left")
        .filter(F.col("amount")> F.col("transaction_limit"))
        .select(
            F.concat_ws("-",F.lit("ALERT"),F.col("transaction_id")).alias("alert_id")
            ,F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type")
            ,F.current_timestamp().alias("alert_timestamp")
            ,transactions.transaction_id
            ,transactions.customer_id
            ,customers.email.alias("customer_email")
            ,F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name")
            ,transactions.amount.alias("transaction_amount")
            ,customers.transaction_limit
            ,transactions.currency
            ,transactions.merchant_name
            ,transactions.merchant_category
            ,transactions.transaction_type
            ,transactions.payment_channel
            ,transactions.city
            ,transactions.country
            ,transactions.is_international
            ,transactions.transaction_timestamp
            ,transactions.status
        )
    )
    return tran_cust_df