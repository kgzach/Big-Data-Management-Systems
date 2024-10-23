#### Ερώτημα 1.
from pyspark.sql.functions import col


def saveToMongo(df, db_uri, db_name, collection_name):
    transformed_df = df.withColumn("processed_value", col("json"))
    transformed_df.write \
        .format("mongodb") \
        .mode("append") \
        .option("spark.mongodb.output.uri", f"{db_uri}/{db_name}.{collection_name}") \
        .save()
"""
.option("database", "") \
.option("collection", "") 
.save()"""
