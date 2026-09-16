# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # IoT synthetic load generator
# MAGIC
# MAGIC Appends distributed synthetic telemetry and machine-failure events to the
# MAGIC Delta tables created by `ddl.sql`. Edit the constants below, attach Spark
# MAGIC compute, and run all cells. Repeated runs append new data.

# COMMAND ----------

from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F

# Target tables.
CATALOG = "exalabs"
SCHEMA = "iot"
TELEMETRY_TABLE = "iot_sample_data"
FAILURE_TABLE = "machine_failures"
ENRICHED_FAILURE_TABLE = "failure_details"

# Load profile. The default run appends 100 million telemetry rows in 20 Delta
# commits, followed by 10,000 failure rows in one commit.
TOTAL_TELEMETRY_ROWS = 100_000_000
TELEMETRY_BATCH_ROWS = 5_000_000
FAILURE_ROWS = 10_000
OUTPUT_PARTITIONS = 400

# Synthetic fleet shape and event-time coverage.
CUSTOMER_COUNT = 1_000
SITE_COUNT = 5_000
MACHINE_COUNT = 100_000
RACKS_PER_SITE = 50
ROOMS_PER_SITE = 20
HARDWARE_CONFIG_COUNT = 25
COMPONENTS_PER_ASSET = 6
FAILURE_MODE_COUNT = 40
EVENT_TIME_SPAN_DAYS = 90  # Spread each machine's readings over this history.
LIVE_EVENT_INTERVAL_SECONDS = 30  # Used only when EVENT_TIME_SPAN_DAYS is 0.
SEED = 42

# Historical mode ends at the current UTC time. Set EVENT_TIME_SPAN_DAYS to 0
# for a forward-moving live window beginning at RUN_END.
RUN_END = datetime.now(timezone.utc).replace(microsecond=0)
RUN_START = RUN_END - timedelta(days=EVENT_TIME_SPAN_DAYS)

assert TOTAL_TELEMETRY_ROWS >= 0
assert TELEMETRY_BATCH_ROWS > 0
assert FAILURE_ROWS >= 0
assert OUTPUT_PARTITIONS > 0
assert CUSTOMER_COUNT > 0
assert SITE_COUNT > 0
assert MACHINE_COUNT > 0
assert RACKS_PER_SITE > 0
assert ROOMS_PER_SITE > 0
assert HARDWARE_CONFIG_COUNT > 0
assert EVENT_TIME_SPAN_DAYS >= 0
assert LIVE_EVENT_INTERVAL_SECONDS > 0

TELEMETRY_FQN = f"{CATALOG}.{SCHEMA}.{TELEMETRY_TABLE}"
FAILURE_FQN = f"{CATALOG}.{SCHEMA}.{FAILURE_TABLE}"
ENRICHED_FAILURE_FQN = f"{CATALOG}.{SCHEMA}.{ENRICHED_FAILURE_TABLE}"
RUN_START_EPOCH_SECONDS = int(RUN_START.timestamp())
SAMPLES_PER_MACHINE = max(1, (TOTAL_TELEMETRY_ROWS + MACHINE_COUNT - 1) // MACHINE_COUNT)
EVENT_STEP_SECONDS = (
    max(1, EVENT_TIME_SPAN_DAYS * 86_400 // SAMPLES_PER_MACHINE)
    if EVENT_TIME_SPAN_DAYS > 0
    else LIVE_EVENT_INTERVAL_SECONDS
)
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate the target model

# COMMAND ----------

EXPECTED_TELEMETRY_COLUMNS = [
    "customer_id",
    "site_id",
    "machine_id",
    "serial_number",
    "firmware_version",
    "latitude",
    "longitude",
    "temperature",
    "cpu_load",
    "network_latency",
    "rack_id",
    "room_id",
    "hardware_configuration_id",
    "event_time",
]
EXPECTED_FAILURE_COLUMNS = ["machine_id", "failure_time"]
EXPECTED_ENRICHED_FAILURE_COLUMNS = [
    "machine_id", "failure_time", "asset_component_id", "failure_mode_id",
    "failure_started_at", "confirmed_at", "restored_at",
    "severity", "detection_method", "root_cause", "planned",
    "downtime_minutes", "production_impact",
]


def validate_table(table_fqn, expected_columns):
    if not spark.catalog.tableExists(table_fqn):
        raise RuntimeError(
            f"Required table {table_fqn} does not exist. Run iot/ddl.sql first."
        )

    actual_columns = spark.table(table_fqn).columns
    if actual_columns != expected_columns:
        raise RuntimeError(
            f"Unexpected schema for {table_fqn}. "
            f"Expected {expected_columns}, found {actual_columns}."
        )


validate_table(TELEMETRY_FQN, EXPECTED_TELEMETRY_COLUMNS)
validate_table(FAILURE_FQN, EXPECTED_FAILURE_COLUMNS)
validate_table(ENRICHED_FAILURE_FQN, EXPECTED_ENRICHED_FAILURE_COLUMNS)
print(f"Validated {TELEMETRY_FQN}, {FAILURE_FQN}, and {ENRICHED_FAILURE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Synthetic telemetry

# COMMAND ----------


def positive_hash(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(2_147_483_647))


def telemetry_batch(batch_start, batch_rows):
    """Build one distributed telemetry batch without driver-side row creation."""
    base = (
        spark.range(batch_rows, numPartitions=OUTPUT_PARTITIONS)
        .withColumn("row_id", F.col("id") + F.lit(batch_start))
        .withColumn(
            "machine_id",
            (F.pmod(F.col("row_id"), F.lit(MACHINE_COUNT)) + 1).cast("int"),
        )
        .withColumn(
            "sample_sequence",
            F.floor(F.col("row_id") / F.lit(MACHINE_COUNT)).cast("long"),
        )
        .withColumn(
            "site_id",
            (F.pmod(F.col("machine_id") - 1, F.lit(SITE_COUNT)) + 1).cast("int"),
        )
    )

    machine_hash = positive_hash(F.col("machine_id"))
    reading_hash = positive_hash(F.col("machine_id"), F.col("sample_sequence"))
    site_hash = positive_hash(F.col("site_id"))
    event_epoch = (
        F.lit(RUN_START_EPOCH_SECONDS)
        + F.col("sample_sequence") * F.lit(EVENT_STEP_SECONDS)
    )
    daily_phase = F.col("sample_sequence") * F.lit(
        EVENT_STEP_SECONDS * 2.0 * 3.141592653589793 / 86_400.0
    )

    return base.select(
        (F.pmod(F.col("site_id") - 1, F.lit(CUSTOMER_COUNT)) + 1)
        .cast("int")
        .alias("customer_id"),
        F.col("site_id"),
        F.col("machine_id"),
        F.format_string("SN-%010d", F.col("machine_id")).alias("serial_number"),
        F.concat(
            F.lit("v"),
            (F.pmod(machine_hash, F.lit(4)) + 1).cast("string"),
            F.lit("."),
            F.pmod(F.floor(machine_hash / 4), F.lit(10)).cast("string"),
            F.lit("."),
            F.pmod(F.floor(machine_hash / 40), F.lit(20)).cast("string"),
        ).alias("firmware_version"),
        (
            F.lit(-60.0)
            + F.pmod(site_hash, F.lit(1_200_000)).cast("double") / 10_000.0
        ).alias("latitude"),
        (
            F.lit(-170.0)
            + F.pmod(F.floor(site_hash / 7), F.lit(3_400_000)).cast("double")
            / 10_000.0
        ).alias("longitude"),
        (
            F.lit(22.0)
            + F.sin(daily_phase) * 6.0
            + (F.pmod(reading_hash, F.lit(1_000)).cast("double") / 100.0 - 5.0)
            + F.when(F.pmod(reading_hash, F.lit(2_000)) == 0, 35.0).otherwise(0.0)
        ).cast("double").alias("temperature"),
        F.least(
            F.lit(100.0),
            (
                F.pmod(reading_hash, F.lit(8_500)).cast("double") / 100.0
                + F.when(F.pmod(reading_hash, F.lit(500)) == 0, 25.0).otherwise(0.0)
            ),
        ).cast("double").alias("cpu_load"),
        (
            F.lit(2.0)
            + F.pmod(F.floor(reading_hash / 11), F.lit(9_800)).cast("double") / 100.0
            + F.when(F.pmod(reading_hash, F.lit(1_000)) == 0, 250.0).otherwise(0.0)
        ).cast("double").alias("network_latency"),
        (F.pmod(F.col("machine_id") - 1, F.lit(RACKS_PER_SITE)) + 1)
        .cast("int")
        .alias("rack_id"),
        (F.pmod(F.col("machine_id") - 1, F.lit(ROOMS_PER_SITE)) + 1)
        .cast("int")
        .alias("room_id"),
        (F.pmod(machine_hash, F.lit(HARDWARE_CONFIG_COUNT)) + 1)
        .cast("int")
        .alias("hardware_configuration_id"),
        F.timestamp_seconds(event_epoch).alias("event_time"),
    )


telemetry_written = 0
batch_number = 0
while telemetry_written < TOTAL_TELEMETRY_ROWS:
    current_batch_rows = min(
        TELEMETRY_BATCH_ROWS, TOTAL_TELEMETRY_ROWS - telemetry_written
    )
    batch_number += 1
    print(
        f"Appending telemetry batch {batch_number}: "
        f"rows {telemetry_written:,} through "
        f"{telemetry_written + current_batch_rows - 1:,}"
    )
    (
        telemetry_batch(telemetry_written, current_batch_rows)
        .write.format("delta")
        .mode("append")
        .saveAsTable(TELEMETRY_FQN)
    )
    telemetry_written += current_batch_rows

print(f"Appended {telemetry_written:,} telemetry rows in {batch_number} batches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Machine failures

# COMMAND ----------


def failure_event_batch(row_count):
    if row_count == 0:
        return None

    # Spread failures across the same time horizon as the telemetry run. A
    # machine may fail more than once, which is useful for event-heavy tests.
    failure_window_seconds = SAMPLES_PER_MACHINE * EVENT_STEP_SECONDS
    base = spark.range(row_count, numPartitions=min(OUTPUT_PARTITIONS, row_count))
    failure_hash = positive_hash(F.col("id"), F.lit("failure"))
    failure_time_hash = positive_hash(F.col("id"), F.lit("failure_time"))

    return base.select(
        F.col("id"),
        failure_hash.alias("failure_hash"),
        (F.pmod(failure_hash, F.lit(MACHINE_COUNT)) + 1)
        .cast("int")
        .alias("machine_id"),
        F.timestamp_seconds(
            F.lit(RUN_START_EPOCH_SECONDS)
            + F.pmod(
                failure_time_hash,
                F.lit(failure_window_seconds),
            )
        ).alias("failure_time"),
    )


failure_batch = failure_event_batch(FAILURE_ROWS)
if failure_batch is not None:
    failure_batch = failure_batch.cache()
    (
        failure_batch.select("machine_id", "failure_time")
        .write.format("delta")
        .mode("append")
        .saveAsTable(FAILURE_FQN)
    )

    enriched_failures = (
        failure_batch
        .withColumn(
            "downtime_minutes",
            (15.0 + F.pmod(F.floor(F.col("failure_hash") / 17), F.lit(2_865))).cast("double"),
        )
        .select(
            F.col("machine_id"),
            F.col("failure_time"),
            (((F.col("machine_id") - 1) * COMPONENTS_PER_ASSET) + F.pmod(F.col("failure_hash"), F.lit(COMPONENTS_PER_ASSET)) + 1).cast("bigint").alias("asset_component_id"),
            (F.pmod(F.floor(F.col("failure_hash") / 5), F.lit(FAILURE_MODE_COUNT)) + 1).cast("int").alias("failure_mode_id"),
            (F.col("failure_time") - F.expr("INTERVAL 5 MINUTES")).alias("failure_started_at"),
            (F.col("failure_time") + F.expr("INTERVAL 10 MINUTES")).alias("confirmed_at"),
            F.timestamp_seconds((F.unix_timestamp("failure_time") + F.col("downtime_minutes") * 60).cast("long")).alias("restored_at"),
            F.when(F.col("downtime_minutes") > 1_440, "critical").when(F.col("downtime_minutes") > 240, "major").otherwise("minor").alias("severity"),
            F.lit("telemetry").alias("detection_method"),
            F.element_at(F.array(F.lit("wear"), F.lit("overload"), F.lit("lubrication"), F.lit("electrical")), (F.pmod(F.col("failure_hash"), F.lit(4)) + 1).cast("int")).alias("root_cause"),
            F.lit(False).alias("planned"),
            F.col("downtime_minutes"),
            (F.col("downtime_minutes") * (10.0 + F.pmod(F.col("failure_hash"), F.lit(90)))).alias("production_impact"),
        )
    )
    (
        enriched_failures.write.format("delta")
        .mode("append")
        .saveAsTable(ENRICHED_FAILURE_FQN)
    )
    failure_batch.unpersist()
print(f"Appended {FAILURE_ROWS:,} machine-failure rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run summary

# COMMAND ----------

summary = spark.createDataFrame(
    [
        (TELEMETRY_FQN, telemetry_written, batch_number, RUN_START),
        (FAILURE_FQN, FAILURE_ROWS, 1 if FAILURE_ROWS else 0, RUN_START),
        (ENRICHED_FAILURE_FQN, FAILURE_ROWS, 1 if FAILURE_ROWS else 0, RUN_START),
    ],
    "table_name string, appended_rows long, delta_commits int, run_start timestamp",
)
display(summary)