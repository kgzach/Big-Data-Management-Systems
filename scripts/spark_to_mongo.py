#### Ερώτημα 3.3
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, from_json, count, avg, lit, when


def processDataframe(df, schema):
    parsed_df = df.withColumn("json_data", from_json(col("value").cast("string"), schema)) \
        .select("json_data.*")  # Expand JSON fields into individual columns
    # helps with average, counts 0 instead of -1
    filtered_df = parsed_df.withColumn("speed", when(col("speed") < 0, 0).otherwise(col("speed")))
    first_time = filtered_df.select("time").first()["time"] #selecting the time of the first for the db entry
    processed_df = parsed_df.groupBy("link").agg(
        count("name").cast(IntegerType()).alias("vcount"),
        avg("speed").alias("vspeed")
    )
    processed_df = processed_df.withColumn("time", lit(first_time).cast("string"))
    return processed_df

def rawDataframe(df, schema):
    parsed_df = df.withColumn("json_data", from_json(col("value").cast("string"), schema)) \
        .select("json_data.*")
    return parsed_df

def saveToMongo(df, db_uri, db_name, collection_name):
    try:
        print(f"Saving batch to MongoDB: {db_uri}/{db_name}.{collection_name}")
        df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", db_uri) \
            .option("database", "BigData") \
            .option("collection", collection_name) \
            .save()
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")
#"spark.mongodb.output.uri" .option("uri", f"{db_uri}/{db_name}.{collection_name}") \

def processAndSaveBatch(df, epoch_id, db_uri, db_name, schema,
            raw_data_collection_name, processed_data_collection_name):
    if df.count() > 0:
        print(f"Epoch {epoch_id} processed")
        raw_df = rawDataframe(df, schema)
        saveToMongo(raw_df, db_uri, db_name, raw_data_collection_name)
        processed_df = processDataframe(df, schema)
        saveToMongo(processed_df, db_uri, db_name, processed_data_collection_name)
    else:
        print(f"Batch {epoch_id} is empty")
