# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Dexcom EGV Auto Loader -> `exalabs.dexcom.egv_readings`
# MAGIC Ingests Dexcom JSON files landing in `/Volumes/exalabs/dexcom/uploads/` (written by `dexcom_fetch.py`
# MAGIC and `push_to_volume.py`) and MERGEs them into Delta, deduped on `record_id`.
# MAGIC Run by a file-arrival job; each run processes only files it has not seen (`availableNow`).

# COMMAND ----------

CATALOG, SCHEMA = "exalabs", "dexcom"
SOURCE = "/Volumes/exalabs/dexcom/uploads/"
TARGET = f"{CATALOG}.{SCHEMA}.egv_readings"
CHECKPOINT_VOLUME = f"{CATALOG}.{SCHEMA}.checkpoints"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/egv_autoloader"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CHECKPOINT_VOLUME}")  # checkpoints live outside the landing folder
spark.sql(f"""CREATE TABLE IF NOT EXISTS {TARGET} (
  record_id STRING, user_id STRING, system_time TIMESTAMP, display_time TIMESTAMP_NTZ,
  glucose_mgdl INT, trend STRING, trend_rate DOUBLE, unit STRING, rate_unit STRING,
  transmitter_generation STRING, display_device STRING, display_app STRING, ingested_at TIMESTAMP)""")

# COMMAND ----------

# Shape of the file written by dexcom_fetch.py (the Dexcom response plus fetchedAt)
FILE_SCHEMA = """
  userId STRING, fetchedAt STRING,
  records ARRAY<STRUCT<recordId: STRING, systemTime: STRING, displayTime: STRING, value: INT,
                       trend: STRING, trendRate: DOUBLE, unit: STRING, rateUnit: STRING,
                       transmitterGeneration: STRING, displayDevice: STRING, displayApp: STRING>>
"""

def merge_batch(batch_df, batch_id):
    spark_s = batch_df.sparkSession
    batch_df.createOrReplaceTempView("incoming_files")
    spark_s.sql(f"""
        MERGE INTO {TARGET} t
        USING (
          SELECT record_id, user_id, system_time, display_time, glucose_mgdl, trend, trend_rate,
                 unit, rate_unit, transmitter_generation, display_device, display_app,
                 current_timestamp() AS ingested_at
          FROM (
            SELECT r.recordId AS record_id, f.userId AS user_id,
                   to_timestamp(r.systemTime) AS system_time,
                   CAST(substring(r.displayTime, 1, 19) AS TIMESTAMP_NTZ) AS display_time,
                   r.value AS glucose_mgdl, r.trend AS trend, r.trendRate AS trend_rate,
                   r.unit AS unit, r.rateUnit AS rate_unit,
                   r.transmitterGeneration AS transmitter_generation,
                   r.displayDevice AS display_device, r.displayApp AS display_app,
                   row_number() OVER (PARTITION BY r.recordId ORDER BY f.fetchedAt DESC) AS rn
            FROM incoming_files f LATERAL VIEW explode(f.records) x AS r
            WHERE r.value IS NOT NULL
          ) WHERE rn = 1
        ) s
        ON t.record_id = s.record_id
        WHEN NOT MATCHED THEN INSERT *""")

(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("multiLine", "true")
    .option("pathGlobFilter", "egv_*.json")
    .schema(FILE_SCHEMA)
    .load(SOURCE)
  .writeStream
    .foreachBatch(merge_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
    .awaitTermination())

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS readings, min(system_time) AS first_reading, max(system_time) AS last_reading FROM {TARGET}"))