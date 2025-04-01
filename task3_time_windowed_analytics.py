from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os

# 1. Start Spark session
spark = SparkSession.builder.appName("RideSharingTask3").getOrCreate()

# 2. Define the input schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # raw string format initially

# 3. Read raw data from socket
raw_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse the JSON and convert timestamp
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", col("timestamp").cast(TimestampType()))

# 5. Perform windowed aggregation on fare_amount
windowed_df = parsed_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).sum("fare_amount").withColumnRenamed("sum(fare_amount)", "total_fare")

# ✅ 6. Flatten the window struct into separate columns
flattened_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# 7. Batch writer to CSV
def write_to_csv(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():
        output_path = f"./output_task3/batch_{batch_id}"
        os.makedirs(output_path, exist_ok=True)
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f" Batch {batch_id} written to {output_path}")
    else:
        print(f"  Batch {batch_id} is empty – skipping.")

# 8. Write the flattened windowed data
query = flattened_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "./chk_task3") \
    .start()

query.awaitTermination()
