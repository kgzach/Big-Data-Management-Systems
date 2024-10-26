#### Ερώτημα 3.3
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import col, from_json, split, avg, count, avg, to_timestamp, current_timestamp

def processDataframe(df, schema):
    #parsed_df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    parsed_df = df.withColumn("json_data", from_json(col("value").cast("string"), schema)) \
        .select("json_data.*")  # Expand JSON fields into individual columns

    processed_df = parsed_df.groupBy("link").agg(
        count("name").alias("vcount"),
        avg("speed").alias("vspeed")
    )
    #processed_df = processed_df.withColumn("time", col("t").cast(TimestampType()))
    return processed_df

def saveToMongo(df, db_uri, db_name, collection_name):
    try:
        print(f"Saving batch to MongoDB: {db_uri}/{db_name}.{collection_name}")
        df.write \
            .format("mongodb") \
            .mode("append") \
            .option("uri", db_uri) \
            .option("database", db_name) \
            .option("collection", collection_name) \
            .save()
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
#"spark.mongodb.output.uri" .option("uri", f"{db_uri}/{db_name}.{collection_name}") \

def processAndSaveBatch(df, epoch_id, db_uri, db_name, schema, raw_data_collection_name, processed_data_collection_name):
    if df.count() > 0:
        print(f"Epoch {epoch_id} processed")
        saveToMongo(df, db_uri, db_name, raw_data_collection_name)
        processed_df = processDataframe(df, schema)
        saveToMongo(processed_df, db_uri, db_name, processed_data_collection_name)
    else:
        print(f"Batch {epoch_id} is empty")
