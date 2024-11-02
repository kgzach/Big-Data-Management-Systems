from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SparkConnectionTest") \
    .getOrCreate()

try:
    data = [("Alice", 1), ("Bob", 2)]
    df = spark.createDataFrame(data, ["name", "id"])
    df.show()
    count = df.count()
    print(f"DataFrame row count: {count}")
    print("✓ Spark connection test successful.")

except Exception as e:
    print(f"Spark connection test failed: {e}")

finally:
    spark.stop()
