# Databricks notebook source
# MAGIC %md
# MAGIC # Ride Sharing Analytics
# MAGIC **Ride Requests → Kafka (Kubernetes on laptop via ngrok, or on EC2) → Databricks Structured Streaming → Delta tables → S3**
# MAGIC
# MAGIC Run the cells top to bottom. Each run picks up only the *new* rides since the last run
# MAGIC (the checkpoint remembers where it stopped). Schedule this notebook as a Job every
# MAGIC 5–10 minutes to make it near real-time.

# COMMAND ----------

# MAGIC %pip install boto3 --quiet

# COMMAND ----------

# ---------- STEP 0: Settings (edit these) ----------
KAFKA_BOOTSTRAP = "PASTE_ADDRESS_HERE"   # printed by the deploy script
                                         # laptop: the Pinggy address, e.g. abcde.a.free.pinggy.link:41234   EC2: <public-ip>:30094
TOPIC = "ride-requests"

CATALOG, SCHEMA = "workspace", "default"        # Free Edition defaults
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.rides_bronze"

# S3 export (Step 5)
AWS_ACCESS_KEY_ID = "PASTE_ACCESS_KEY"          # demo only: never push keys to GitHub
AWS_SECRET_ACCESS_KEY = "PASTE_SECRET_KEY"
AWS_REGION = "ap-south-1"
S3_BUCKET = "PASTE_BUCKET_NAME"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.ride_checkpoints")
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/ride_checkpoints/bronze"
print("Settings loaded")

# COMMAND ----------

# ---------- STEP 1: Can Databricks reach Kafka on EC2? ----------
import socket

host, port = KAFKA_BOOTSTRAP.split(":")
try:
    s = socket.create_connection((host, int(port)), timeout=10)
    s.close()
    print(f"SUCCESS: Databricks can reach Kafka at {KAFKA_BOOTSTRAP}")
except Exception as e:
    print(f"FAILED to reach {KAFKA_BOOTSTRAP}: {e}")
    print("Check: Is the tunnel (laptop) / the EC2 running? Address pasted correctly? "
          "EC2 only: security group allows port 30094? "
          "Databricks LinkedIn verification done (outbound internet)?")

# COMMAND ----------

# ---------- OPTIONAL RESET: run ONLY after Kafka was restarted/redeployed ----------
# Kafka loses its old messages when it restarts, but the checkpoint still remembers the
# old position. Deleting the checkpoint makes Step 2 read the new Kafka from the start.
# (Old rides already saved in rides_bronze stay there.)
RESET_CHECKPOINT = False   # change to True, run this cell once, then set back to False
if RESET_CHECKPOINT:
    dbutils.fs.rm(CHECKPOINT, True)
    print("Checkpoint deleted - Step 2 will start fresh from the current Kafka")

# COMMAND ----------

# ---------- STEP 2: Read the Kafka stream and save raw rides (Bronze) ----------
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ride_schema = StructType([
    StructField("ride_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("rider_id", StringType()),
    StructField("pickup_zone", StringType()),
    StructField("drop_zone", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("distance_km", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("fare", DoubleType()),
    StructField("status", StringType()),
    StructField("request_time", StringType()),
])

raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

rides_stream = (
    raw_stream
    .select(from_json(col("value").cast("string"), ride_schema).alias("r"),
            col("timestamp").alias("kafka_received_at"))
    .select("r.*", "kafka_received_at")
    .withColumn("request_time", to_timestamp("request_time"))
)

query = (
    rides_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)      # process all new rides, then stop (serverless-friendly)
    .toTable(BRONZE_TABLE)
)
query.awaitTermination()

print(f"Total rides stored so far: {spark.table(BRONZE_TABLE).count()}")

# COMMAND ----------

# ---------- STEP 3: Analytics ----------
from pyspark.sql.functions import (hour, count, sum as _sum, avg, round as _round,
                                   when, desc)

rides = spark.table(BRONZE_TABLE)
completed = rides.filter(col("status") == "completed")

# 3a. Summary KPIs
summary = rides.agg(
    count("*").alias("total_requests"),
    _sum(when(col("status") == "completed", 1).otherwise(0)).alias("completed_rides"),
    _round(100 * _sum(when(col("status") == "cancelled", 1).otherwise(0)) / count("*"), 1)
        .alias("cancellation_rate_pct"),
    _round(_sum("fare"), 2).alias("total_revenue"),
    _round(avg(when(col("status") == "completed", col("fare"))), 2).alias("avg_fare"),
)

# 3b. Peak hours: ride requests per hour of the day
peak_hours = (
    rides.groupBy(hour("request_time").alias("hour_of_day"))
    .agg(count("*").alias("ride_requests"),
         _round(_sum("fare"), 2).alias("revenue"))
    .orderBy("hour_of_day")
)

# 3c. Most active drivers (top 10)
top_drivers = (
    completed.groupBy("driver_id")
    .agg(count("*").alias("rides_completed"),
         _round(_sum("fare"), 2).alias("earnings"),
         _round(_sum("distance_km"), 1).alias("total_km"))
    .orderBy(desc("rides_completed"))
    .limit(10)
)

# 3d. Revenue by pickup zone and by vehicle type
revenue_by_zone = (
    completed.groupBy("pickup_zone")
    .agg(count("*").alias("rides"), _round(_sum("fare"), 2).alias("revenue"))
    .orderBy(desc("revenue"))
)
revenue_by_vehicle = (
    completed.groupBy("vehicle_type")
    .agg(count("*").alias("rides"), _round(_sum("fare"), 2).alias("revenue"),
         _round(avg("fare"), 2).alias("avg_fare"))
    .orderBy(desc("revenue"))
)

results = {
    "summary_kpis": summary,
    "peak_hours": peak_hours,
    "top_drivers": top_drivers,
    "revenue_by_zone": revenue_by_zone,
    "revenue_by_vehicle": revenue_by_vehicle,
}

# Save each result as a Gold table (handy for a Databricks dashboard)
for name, df in results.items():
    df.write.mode("overwrite").option("overwriteSchema", "true") \
      .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_{name}")
print("Gold tables saved:", ", ".join(f"gold_{n}" for n in results))

# COMMAND ----------

# MAGIC %md ### Results (click the chart icon under each table to turn it into a bar/line chart)

# COMMAND ----------

display(summary)

# COMMAND ----------

display(peak_hours)      # suggested chart: bar, X = hour_of_day, Y = ride_requests

# COMMAND ----------

display(top_drivers)     # suggested chart: bar, X = driver_id, Y = rides_completed

# COMMAND ----------

display(revenue_by_zone) # suggested chart: bar, X = pickup_zone, Y = revenue

# COMMAND ----------

display(revenue_by_vehicle)

# COMMAND ----------

# ---------- STEP 5: Export results + raw rides to S3 ----------
import boto3
from datetime import datetime

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

run_stamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
exports = dict(results)
exports["raw_rides"] = rides

for name, df in exports.items():
    csv_text = df.toPandas().to_csv(index=False)
    key = f"ride-analytics/{run_stamp}/{name}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_text.encode("utf-8"))
    print(f"Uploaded s3://{S3_BUCKET}/{key}")
