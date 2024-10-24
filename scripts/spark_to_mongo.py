#### Ερώτημα 3.3
from pyspark.sql.functions import col


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
