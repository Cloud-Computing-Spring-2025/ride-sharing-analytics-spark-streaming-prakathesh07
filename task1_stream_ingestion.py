from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Initialize Spark Session
spark = SparkSession.builder.appName("RideSharingTask1").getOrCreate()

# Define schema of incoming data
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# Read stream from socket
raw_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON data into structured DataFrame
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Write parsed data to CSV (using foreachBatch for better control)
def write_to_csv(batch_df, batch_id):
    batch_df.write.mode("overwrite").csv(f"./output_task1/batch_{batch_id}")

query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "./chk_task1") \
    .start()

query.awaitTermination()
