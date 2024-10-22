import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from spark_to_mongo import saveToMongo
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType


load_dotenv()
kafka_broker = os.getenv('OFFLINE_BROKER')
db_path = os.getenv('DB_PATH')
mongo_uri = os.getenv('MONGO_URI')
topic_name=os.getenv('TOPIC_NAME')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_DB_COLLECTION')

spark = SparkSession.builder \
    .appName(topic_name) \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0") \
    .config("spark.mongodb.output.uri", mongo_uri ) \
    .getOrCreate()
#.config("spark.driver.host", spark_driver) \
spark_driver = os.getenv('SPARK_DRIVER')
if spark_driver is None:
    spark_driver = spark.conf.get("spark.driver.host")
print("Loading spark session...")

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .load()

lines = lines \
    .selectExpr("CAST(value AS STRING) as json")

query = lines \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: saveToMongo(df, db_path, db_name, collection_name)) \
    .format("console") \
    .start()
# Terminates the stream on abort
query.awaitTermination()
