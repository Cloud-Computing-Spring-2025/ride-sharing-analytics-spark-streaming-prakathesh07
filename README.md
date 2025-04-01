
#  Real-Time Ride-Sharing Analytics with Apache Spark

This project simulates and analyzes real-time ride-sharing data using **Apache Spark Structured Streaming**. You'll process JSON-formatted ride events, perform driver-level aggregations, and analyze fare trends over time — all in real time!

##  Project Structure
ride-sharing-analytics-spark-streaming/ 
 ├── data_generator.py # Simulates ride events and sends to socket 
 ├── task1_stream_ingestion.py # Ingests and parses raw streaming JSON 
 ├── task2_driver_aggregations.py # Aggregates fare and distance per driver 
 ├── task3_time_windowed_analytics.py # Aggregates fare over sliding time window 
 ├── output_task1/ # Output files from Task 1 
 ├── output_task2/ # Output files from Task 2 
 ├── output_task3/ # Output files from Task 3 
 ├── chk_task*/ # Checkpoint directories (used by Spark) 
 └── README.md # This file

##Install Dependencies##
pip install faker

pip install pyspark


# Run the data generator to simulate streaming events:

python data_generator.py **It sends one ride event every second to localhost:9999.**
# Task 1: Ingest and Parse Ride Events
📄 File: task1_stream_ingestion.py

**✅ What it does:**
Reads real-time JSON strings from socket

Parses the data into structured columns

Writes parsed data to CSV (1 file per micro-batch)

**▶️ Run:**

python task1_stream_ingestion.py

**📁 Output:**
Check output_task1/batch_<batch_id>/
Each CSV contains:
trip_id,driver_id,distance_km,fare_amount,timestamp
443d50af-8aaa-47b8-af98-f20d53f98c58,28,26.08,136.56,2025-04-01 17:23:54

# Task 2: Driver-Level Aggregations

📄 File: task2_driver_aggregations.py

**✅ What it does:**
Aggregates:

Total fare_amount per driver_id

Average distance_km per driver_id

Saves each batch's output to CSV

# ▶️ Run:

python task2_driver_aggregations.py
# 📁 Output:
Check output_task2/batch_<batch_id>/
Each CSV contains:

driver_id,total_fare,avg_distance
42,132.57,17.43
73,83.33,49.12
34,8.46,23.26
99,108.64,9.12
5,90.21000000000001,8.385
27,150.52,22.12
75,12.5,43.07
41,274.52,19.52
38,47.9,17.66
44,146.10000000000002,13.02
53,144.39,30.93
56,123.81,48.73
10,108.09,41.55


**Task 3: Time-Windowed Fare Analytics**
📄 File: task3_time_windowed_analytics.py

# ✅ What it does:
Converts timestamp to event time

Applies a 5-minute window, sliding every 1 minute

Aggregates SUM(fare_amount) per window

Saves output to CSV

# ▶️ Run:

python task3_time_windowed_analytics.py
# 📁 Output:
Check output_task3/batch_<batch_id>/
Each CSV contains:

window_start,window_end,total_fare
window_start,window_end,total_fare
2025-04-01T17:35:00.000Z,2025-04-01T17:40:00.000Z,1525.0700000000002
2025-04-01T17:31:00.000Z,2025-04-01T17:36:00.000Z,7753.790000000001
2025-04-01T17:33:00.000Z,2025-04-01T17:38:00.000Z,7753.790000000001
2025-04-01T17:34:00.000Z,2025-04-01T17:39:00.000Z,6654.860000000001
2025-04-01T17:32:00.000Z,2025-04-01T17:37:00.000Z,7753.790000000001


**Thank you!!**
