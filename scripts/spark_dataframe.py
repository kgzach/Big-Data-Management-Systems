import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, SubscribeType

load_dotenv()

kafka_broker = os.getenv('KAFKA_BROKER')
db_path = os.getenv('DB_PATH')
topic_name=os.getenv('TOPIC_NAME')

spark = SparkSession.builder \
    .appName(topic_name) \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option(StructType, topic_name) \
    .load()

lines = lines \
    .selectExpr("CAST(value AS STRING) as json")

query = lines \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
# Terminates the stream on abort
query.awaitTermination()