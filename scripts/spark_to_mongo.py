#### Ερώτημα 3.3
from pyspark.sql.functions import col, from_json, split, avg, count

def processDataframe(df, schema):
    df = df.groupBy("link").agg(
        count("name").alias("vcount"),
        avg("v").alias("vspeed"))
    """    parsed_df = (
            df.withColumn("json_data", from_json(col("value").cast("string"), schema))
            .withColumn("time", col("json_data.t"))
            .withColumn("graduation_year", col("json_data.graduation_year"))
            .withColumn("major", col("json_data.major"))
            .drop(col("json_data"))
            .drop(col("value"))
        )
        split_col = split(parsed_df["student_name"], "XX")
        return (
            parsed_df.withColumn("first_name", split_col.getItem(0))
            .withColumn("last_name", split_col.getItem(1))
            .drop("student_name")
        )
    """
    parsed_df = (
        df.withColumn("json_data", from_json(col("value").cast("string"), schema))
        .withColumn("time", col("json_data.t"))
        .withColumn("link", col("json_data.link"))
        .withColumn("vcount", col("json_data.vcount"))
        .withColumn("vspeed", col("json_data.vspeed"))
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
    )
    return parsed_df

def saveToMongo(df, db_uri, db_name, collection_name):
    print("Saving batch to MongoDB")
    transformed_df = df.withColumn("processed_value", col("json"))
    transformed_df.writeStream \
        .format("mongodb") \
        .option("spark.mongodb.output.uri", f"{db_uri}/{db_name}.{collection_name}") \
        .mode("append") \
        .save()
"""
.option("database", "") \
.option("collection", "") 
.option("checkpointLocation", "/path/to/checkpoint/dir")
.save()"""
