#### Ερώτημα 3.3
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, from_json, split, avg, count

def processDataframe(df, schema):
    parsed_df = df.withColumn("json_data", from_json(col("value").cast("string"), schema)) \
        .select("json_data.*")  # Expand JSON fields into individual columns

    processed_df = parsed_df.groupBy("link").agg(
        count("name").alias("vcount"),
        avg("v").alias("vspeed")
    )

    processed_df = processed_df.withColumn("time", col("t").cast(TimestampType()))
    return processed_df
"""
        .drop(col("json_data"))
        .drop(col("index"))
        .drop(col("name"))
        .drop(col("orig"))
        .drop(col("dest"))
        .drop(col("t"))
        .drop(col("link"))
        .drop(col("x"))
        .drop(col("s"))
        .drop(col("v"))
"""

def saveToMongo(df, db_uri, db_name, collection_name):
    try:
        print(f"Saving batch to MongoDB: {db_uri}/{db_name}.{collection_name}")
        df.write \
            .format("mongodb") \
            .option("uri", f"{db_uri}/{db_name}.{collection_name}") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
#"spark.mongodb.output.uri"
def processAndSaveBatch(df, epoch_id, db_uri, db_name, schema, raw_data_collection_name, processed_data_collection_name):
    print(f"Epoch {epoch_id} processed")
    saveToMongo(df, db_uri, db_name, raw_data_collection_name)
    processed_df = processDataframe(df, schema)
    saveToMongo(processed_df, db_uri, db_name, processed_data_collection_name)

