from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType
import os

# 1. Start Spark session
spark = SparkSession.builder.appName("RideSharingTask2").getOrCreate()

# 2. Define schema for incoming data
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

# 3. Read data from socket
raw_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 4. Parse the incoming JSON
parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# 5. Real-time aggregations
agg_df = parsed_df.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# 6. Define batch writer function
def write_to_csv(batch_df, batch_id):
    if not batch_df.rdd.isEmpty():  # Only write if batch has data
        output_path = f"./output_task2/batch_{batch_id}"
        os.makedirs(output_path, exist_ok=True)
        batch_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)
        print(f"✅ Batch {batch_id} written to {output_path}")
    else:
        print(f"⚠️  Batch {batch_id} is empty – nothing written.")

# 7. Start the stream with foreachBatch
query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "./chk_task2") \
    .start()

# 8. Await termination
query.awaitTermination()
