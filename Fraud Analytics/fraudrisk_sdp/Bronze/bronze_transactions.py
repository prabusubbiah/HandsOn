from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F 
import json

@dp.table(
    name="fraudrisk.01_bronze.transactions_stream",
    comment="RAW Transaction data from Kafka"
)
def bronze_transactions() -> DataFrame:
    kafka_connection_json=dbutils.secrets.get(scope="fraudrisk-scope",key="kafka_connection_details")
    kafka_config=json.loads(kafka_connection_json)
    bootstrap_servers=kafka_config['bootstrap_servers']
    API_KEY=kafka_config['api_key']
    API_SECRET=kafka_config['api_secret']
    TOPIC_NAME=kafka_config['topic'] 

    jaas_config = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'

    stream_df=(spark.readStream.format('kafka')
                .option('kafka.bootstrap.servers',bootstrap_servers)
                .option('subscribe',TOPIC_NAME)
                .option('kafka.security.protocol','SASL_SSL')
                .option('kafka.sasl.mechanism','PLAIN')
                .option('kafka.sasl.jaas.config',jaas_config)
                .option("startingOffsets", "earliest")
                .load()
            )
    parsed_stream_df=stream_df.select(
                col("key").cast("string"),
                col("value").cast("string"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp"),
                col("timestampType"),
                F.current_timestamp().alias("ingestion_timestamp")
            )
    return parsed_stream_df

