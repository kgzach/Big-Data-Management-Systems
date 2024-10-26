### Ερωτήματα 2.2 & 3.1
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from spark_to_mongo import processAndSaveBatch
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, TimestampType


load_dotenv()
kafka_broker = os.getenv('OFFLINE_BROKER')
db_uri = os.getenv('MONGO_URI')
mongo_uri = os.getenv('MONGO_URI')
topic_name=os.getenv('TOPIC_NAME')
db_name = os.getenv('MONGO_DB_NAME')
raw_data_collection_name = os.getenv('MONGO_RAW_DATA_COLLECTION')
processed_data_collection_name = os.getenv('MONGO_PROC_DATA_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
raw_collection = db[raw_data_collection_name]
processed_data_collection = db[processed_data_collection_name]

    #### Ερώτημα 2.2
spark = SparkSession.builder \
    .appName(topic_name) \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.ui.port", "4050") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.mongodb.output.uri", mongo_uri ) \
    .getOrCreate()
#.config("spark.driver.host", spark_driver) \
#.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0") \

spark_driver = os.getenv('SPARK_DRIVER')
if spark_driver is None:
    spark_driver = spark.conf.get("spark.driver.host")
print("Loading spark session...")

    #### Ερώτημα 3.1

schema = StructType([
    StructField("index", StringType(), True),
    StructField("name", StringType(), True),
    StructField("orig", StringType(), True),
    StructField("dest", StringType(), True),
    StructField("t", TimestampType(), True),
    StructField("link", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("s", DoubleType(), True),
    StructField("v", DoubleType(), True)
])

processed_schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("link", StringType(), True),
    StructField("vcount", DoubleType(), True),
    StructField("vspeed", DoubleType(), True)
])

kafka_dataframe = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .load()

query = kafka_dataframe.selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value") \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id:processAndSaveBatch(
        df, epoch_id, db_uri, db_name, schema, raw_data_collection_name, processed_data_collection_name
    )) \
    .format("console") \
    .start()
# Terminates the stream on abort
query.awaitTermination()
