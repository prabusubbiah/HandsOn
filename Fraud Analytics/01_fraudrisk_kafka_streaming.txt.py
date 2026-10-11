# Databricks notebook source
#bootstrap_servers='pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
#API_KEY='7SSQCMBYHJPYBHGQ'
#API_SECRET='cfltcaWqw+lRuSO+TzIr0usgZaHvmeV6qF97pjoNdECjDS59EtVd7VPwyX0mghMw'
#TOPIC_NAME='credit_card_transaction'

# COMMAND ----------

import json

kafka_connection_json=dbutils.secrets.get(scope="fraudrisk-scope",key="kafka_connection_details")
kafka_config=json.loads(kafka_connection_json)
bootstrap_servers=kafka_config['bootstrap_servers']
API_KEY=kafka_config['api_key']
API_SECRET=kafka_config['api_secret']
TOPIC_NAME=kafka_config['topic'] 

# COMMAND ----------

jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'

# COMMAND ----------

batch_df=(spark.read.format('kafka')
            .option('kafka.bootstrap.servers',bootstrap_servers)
            .option('subscribe',TOPIC_NAME)
            .option('kafka.security.protocol','SASL_SSL')
            .option('kafka.sasl.mechanism','PLAIN')
            .option('kafka.sasl.jaas.config',jaas_config)
            .option("startingOffsets", "earliest")
            .load()
)

# COMMAND ----------

from pyspark.sql.functions import col 
parsed_batch=batch_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

parsed_batch.write.mode("overwrite").saveAsTable("fraudrisk.01_bronze.transactions_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from fraudrisk.`01_bronze`.transactions_batch;

# COMMAND ----------

stream_df=(spark.readStream.format('kafka')
            .option('kafka.bootstrap.servers',bootstrap_servers)
            .option('subscribe',TOPIC_NAME)
            .option('kafka.security.protocol','SASL_SSL')
            .option('kafka.sasl.mechanism','PLAIN')
            .option('kafka.sasl.jaas.config',jaas_config)
            .option("startingOffsets", "earliest")
            .load()
)

# COMMAND ----------

from pyspark.sql.functions import col 
parsed_stream_df=stream_df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

streaming_query=(parsed_stream_df.writeStream.format("delta")
.outputMode('append')
.option("checkpointLocation",'/Volumes/fraudrisk/source/transactions/checkpoint/')
.trigger(availableNow=True)
.table("fraudrisk.01_bronze.transactions_streaming")
)
print("Query strarted with Query id:",streaming_query.id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fraudrisk.01_bronze.transactions_streaming;