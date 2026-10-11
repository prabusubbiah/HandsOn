# Databricks notebook source
incoming_path='/Volumes/fraudrisk/source/fraud_watchlist/incoming/'

# COMMAND ----------

# DBTITLE 1,List incoming files
dbutils.fs.ls(incoming_path)

# COMMAND ----------

# DBTITLE 1,Define Auto Loader stream
input_stream=(spark.readStream.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation", "/Volumes/fraudrisk/source/fraud_watchlist/schema/")
.option("cloudFiles.inferColumnTypes", "true")
.load(incoming_path)
)

# COMMAND ----------

# DBTITLE 1,Print input stream schema
print(input_stream.printSchema())

# COMMAND ----------

from pyspark.sql import functions as F
transformed_df=(input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingestion_timestamp")
    )
)

# COMMAND ----------

streaming_query=(transformed_df.writeStream.format("delta")
.outputMode("Append")
.option("checkpointLocation", "/Volumes/fraudrisk/source/fraud_watchlist/checkpoint/")
.trigger(availableNow=True)
.toTable("fraudrisk.01_bronze.fraud_watchlist_batch")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  fraudrisk.01_bronze.fraud_watchlist_batch;