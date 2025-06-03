from pyspark.sql import SparkSession

# Step 1: Start Spark session
spark = SparkSession.builder \
    .appName("SparkGxDemo") \
    .getOrCreate()

# Step 2: Create a sample DataFrame
data = [("Alice", 30), ("Bob", 45), ("Carol", None)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

# Step 3: Save DataFrame as CSV
df.write.mode("overwrite").option("header", "true").csv("data/people.csv")

# Step 4: Show the DataFrame
df.show()

# Step 5: Stop Spark session
spark.stop()
