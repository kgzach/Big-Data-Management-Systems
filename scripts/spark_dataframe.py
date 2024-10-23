### Ερωτήματα 2.2 & 3.1

#### Ερώτημα 2.2
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from spark_to_mongo import saveToMongo
from pyspark.sql.functions import col, avg, count, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, TimestampType


load_dotenv()
kafka_broker = os.getenv('OFFLINE_BROKER')
db_path = os.getenv('DB_PATH')
mongo_uri = os.getenv('MONGO_URI')
topic_name=os.getenv('TOPIC_NAME')
db_name = os.getenv('MONGO_DB_NAME')
collection_name = os.getenv('MONGO_DB_COLLECTION')

client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

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

"""
df_spark = spark.createDataFrame(df) # χρονοβορο
df_spark.show()
df.printSchema()
"""
df = spark.read.option("header", "true").csv("vehicle_data.csv")
#df = df.toDF("name", "origin", "destination", "time", "link", "position", "spacing", "speed", "index")
df.printSchema()
df.show()

df = df.groupBy("_c4").agg(
    count("_c0").alias("vcount"),
    avg("_c6").alias("vspeed")
)
"""df = df.groupBy("link").agg( # "time",
    count("name").alias("vcount"),
    avg("v").alias("vspeed")
)"""

    #### Ερώτημα 3.1

schema = StructType([
    StructField("name", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("time", StringType(), True),
    StructField("link", StringType(), True),
    StructField("position", DoubleType(), True),
    StructField("spacing", DoubleType(), True),
    StructField("speed", DoubleType(), True)
])

lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic_name) \
    .load()



parsed_df = lines.select(from_json(col("value").cast("string"), schema).alias("parsed_value")).select("parsed_value.*")

"""
lines = lines \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("CAST(value AS STRING)"), schema).alias("data")) \
    .select("data")

lines = lines \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

"""

query = lines \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epoch_id: saveToMongo(df, db_path, db_name, collection_name)) \
    .format("console") \
    .start()
# Terminates the stream on abort
query.awaitTermination()
