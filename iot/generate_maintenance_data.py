# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Asset and maintenance history generator
# MAGIC
# MAGIC Creates a relationally consistent synthetic baseline for the entities in
# MAGIC `ddl.sql`. Run this notebook before the append-only telemetry
# MAGIC workload so that telemetry `machine_id` values resolve to `assets.asset_id`.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F

# Target and write behavior.
CATALOG = "exalabs"
SCHEMA = "iot"
WRITE_MODE = "overwrite"  # Baseline generation intentionally replaces data.
OUTPUT_PARTITIONS = 200
SEED = 42

# Fleet shape. MACHINE_COUNT in generate_synthetic_load.py must equal ASSET_COUNT.
CUSTOMER_COUNT = 1_000
SITE_COUNT = 5_000
ASSET_TYPE_COUNT = 8
ASSET_MODEL_COUNT = 40
ASSET_COUNT = 100_000  # Minimum; raised automatically for observed machine IDs.
PART_COUNT = 500
COMPONENTS_PER_ASSET = 6
SENSORS_PER_ASSET = 4
FAILURE_MODES_PER_ASSET_TYPE = 5

# Historical event volumes.
FAILURE_COUNT = 250_000
PREVENTIVE_WORK_ORDER_COUNT = 100_000
STATE_EVENTS_PER_ASSET = 20
METER_READINGS_PER_ASSET = 10

HISTORY_START = datetime(2023, 1, 1, tzinfo=timezone.utc)
HISTORY_DAYS = 1_095

assert WRITE_MODE == "overwrite"
assert OUTPUT_PARTITIONS > 0
assert SITE_COUNT >= CUSTOMER_COUNT
assert ASSET_COUNT > 0
assert ASSET_MODEL_COUNT >= ASSET_TYPE_COUNT
assert FAILURE_COUNT > 0

NAMESPACE = f"{CATALOG}.{SCHEMA}"
HISTORY_START_EPOCH = int(HISTORY_START.timestamp())
HISTORY_SECONDS = HISTORY_DAYS * 86_400
COMPONENT_COUNT = ASSET_COUNT * COMPONENTS_PER_ASSET
SENSOR_COUNT = ASSET_COUNT * SENSORS_PER_ASSET
FAILURE_MODE_COUNT = ASSET_TYPE_COUNT * FAILURE_MODES_PER_ASSET_TYPE
WORK_ORDER_COUNT = FAILURE_COUNT + PREVENTIVE_WORK_ORDER_COUNT
STATE_EVENT_COUNT = ASSET_COUNT * STATE_EVENTS_PER_ASSET
METER_READING_COUNT = ASSET_COUNT * METER_READINGS_PER_ASSET

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers and target validation

# COMMAND ----------

TARGET_TABLES = [
    "iot_sample_data", "customers", "sites", "asset_types", "asset_models", "assets",
    "part_catalog", "asset_components", "sensor_types", "sensors",
    "failure_modes", "failure_details", "work_orders", "maintenance_actions",
    "work_order_parts", "asset_state_events", "meter_readings",
    "maintenance_predictions", "asset_feature_snapshots",
    "maintenance_prediction_outcomes", "machine_failures",
]


def table_name(name):
    return f"{NAMESPACE}.{name}"


missing_tables = [name for name in TARGET_TABLES if not spark.catalog.tableExists(table_name(name))]
if missing_tables:
    raise RuntimeError(
        "Missing target tables: "
        + ", ".join(missing_tables)
        + ". Run ddl.sql first."
    )

# Preserve and include collected facts. The synthetic asset range expands to
# cover every machine already present in either canonical fact table.
telemetry_max_machine = (
    spark.table(table_name("iot_sample_data"))
    .agg(F.max("machine_id").alias("max_machine_id"))
    .first()["max_machine_id"]
    or 0
)
failure_max_machine = (
    spark.table(table_name("machine_failures"))
    .agg(F.max("machine_id").alias("max_machine_id"))
    .first()["max_machine_id"]
    or 0
)
ASSET_COUNT = max(ASSET_COUNT, telemetry_max_machine, failure_max_machine)
COMPONENT_COUNT = ASSET_COUNT * COMPONENTS_PER_ASSET
SENSOR_COUNT = ASSET_COUNT * SENSORS_PER_ASSET
STATE_EVENT_COUNT = ASSET_COUNT * STATE_EVENTS_PER_ASSET
METER_READING_COUNT = ASSET_COUNT * METER_READINGS_PER_ASSET
print(f"Generating asset coverage through machine_id {ASSET_COUNT:,}")


def stable_hash(*columns):
    return F.pmod(F.xxhash64(*columns, F.lit(SEED)), F.lit(2_147_483_647))


def timestamp_at(seconds):
    return F.timestamp_seconds(F.lit(HISTORY_START_EPOCH) + seconds.cast("long"))


def random_history_timestamp(hash_column):
    return timestamp_at(F.pmod(hash_column, F.lit(HISTORY_SECONDS)))


def write_table(name, dataframe, expected_rows):
    partitions = min(OUTPUT_PARTITIONS, max(1, (expected_rows + 249_999) // 250_000))
    target_columns = spark.table(table_name(name)).columns
    if dataframe.columns != target_columns:
        raise RuntimeError(
            f"Column mismatch for {table_name(name)}. "
            f"Expected {target_columns}, generated {dataframe.columns}."
        )
    (
        dataframe.repartition(partitions)
        .write.mode(WRITE_MODE)
        .insertInto(table_name(name))
    )
    print(f"Wrote {name}: {expected_rows:,} rows across {partitions} output partitions")


def merge_new_rows(name, dataframe, key_columns, candidate_rows):
    """Insert missing synthetic facts without replacing collected data."""
    target_columns = spark.table(table_name(name)).columns
    if dataframe.columns != target_columns:
        raise RuntimeError(
            f"Column mismatch for {table_name(name)}. "
            f"Expected {target_columns}, generated {dataframe.columns}."
        )
    view_name = f"synthetic_{name}_source"
    dataframe.dropDuplicates(key_columns).createOrReplaceTempView(view_name)
    join_condition = " AND ".join(f"target.`{key}` = source.`{key}`" for key in key_columns)
    spark.sql(
        f"MERGE INTO {table_name(name)} AS target "
        f"USING {view_name} AS source ON {join_condition} "
        "WHEN NOT MATCHED THEN INSERT *"
    )
    spark.catalog.dropTempView(view_name)
    print(f"Merged up to {candidate_rows:,} candidate rows into {name}")


def integer_key(column, cardinality):
    return (F.pmod(column, F.lit(cardinality)) + 1).cast("int")


results = []


def record_write(name, dataframe, row_count):
    write_table(name, dataframe, row_count)
    results.append((name, row_count))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organization, sites, and asset catalog

# COMMAND ----------

customers = spark.range(CUSTOMER_COUNT).select(
    (F.col("id") + 1).cast("int").alias("customer_id"),
    F.format_string("Customer %05d", F.col("id") + 1).alias("customer_name"),
    F.element_at(
        F.array(*[F.lit(x) for x in ["Manufacturing", "Energy", "Logistics", "Healthcare", "Utilities"]]),
        (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int"),
    ).alias("industry"),
    F.element_at(F.array(F.lit("standard"), F.lit("premium"), F.lit("critical")), (F.pmod(F.col("id"), F.lit(3)) + 1).cast("int")).alias("service_tier"),
    F.lit(True).alias("active"),
    timestamp_at(F.col("id") * 60).alias("created_at"),
)
record_write("customers", customers, CUSTOMER_COUNT)

sites_base = spark.range(SITE_COUNT).withColumn("site_hash", stable_hash(F.col("id")))
sites = sites_base.select(
    (F.col("id") + 1).cast("int").alias("site_id"),
    integer_key(F.col("id"), CUSTOMER_COUNT).alias("customer_id"),
    F.format_string("Site %06d", F.col("id") + 1).alias("site_name"),
    F.element_at(F.array(F.lit("factory"), F.lit("warehouse"), F.lit("data_center"), F.lit("field_station")), (F.pmod(F.col("site_hash"), F.lit(4)) + 1).cast("int")).alias("site_type"),
    F.lit("UTC").alias("timezone"),
    (-60.0 + F.pmod(F.col("site_hash"), F.lit(1_200_000)) / 10_000.0).cast("double").alias("latitude"),
    (-170.0 + F.pmod(F.floor(F.col("site_hash") / 7), F.lit(3_400_000)) / 10_000.0).cast("double").alias("longitude"),
    random_history_timestamp(F.col("site_hash")).alias("commissioned_at"),
    F.lit(True).alias("active"),
)
record_write("sites", sites, SITE_COUNT)

asset_type_rows = [
    (1, "Centrifugal Pump", "fluid_handling", "Moves process liquids"),
    (2, "Air Compressor", "pneumatic", "Supplies compressed air"),
    (3, "Electric Motor", "drive", "Provides rotational power"),
    (4, "Industrial Fan", "air_handling", "Moves or exhausts air"),
    (5, "Generator", "power", "Produces electrical power"),
    (6, "HVAC Unit", "environmental", "Controls facility climate"),
    (7, "Conveyor", "material_handling", "Moves material between stations"),
    (8, "Edge Gateway", "computing", "Aggregates local telemetry"),
]
assert len(asset_type_rows) == ASSET_TYPE_COUNT
asset_types = spark.createDataFrame(asset_type_rows, "asset_type_id int, asset_type_name string, asset_category string, description string")
record_write("asset_types", asset_types, ASSET_TYPE_COUNT)

models_base = spark.range(ASSET_MODEL_COUNT).withColumn("model_hash", stable_hash(F.col("id")))
asset_models = models_base.select(
    (F.col("id") + 1).cast("int").alias("asset_model_id"),
    integer_key(F.col("id"), ASSET_TYPE_COUNT).alias("asset_type_id"),
    F.element_at(F.array(*[F.lit(x) for x in ["Northstar", "Apex", "Vector", "Meridian", "Summit"]]), (F.pmod(F.col("model_hash"), F.lit(5)) + 1).cast("int")).alias("manufacturer"),
    F.format_string("MODEL-%04d", F.col("id") + 1).alias("model_number"),
    (50.0 + F.pmod(F.col("model_hash"), F.lit(20_000)) / 10.0).alias("rated_capacity"),
    F.lit("nominal_units").alias("capacity_unit"),
    (40_000.0 + F.pmod(F.col("model_hash"), F.lit(80_000))).alias("expected_life_hours"),
    (500.0 + F.pmod(F.col("model_hash"), F.lit(2_500))).alias("maintenance_interval_hours"),
)
record_write("asset_models", asset_models, ASSET_MODEL_COUNT)

assets_base = (
    spark.range(ASSET_COUNT)
    .withColumn("asset_hash", stable_hash(F.col("id")))
    .withColumn("site_id", integer_key(F.col("id"), SITE_COUNT))
    .withColumn("asset_model_id", integer_key(F.col("asset_hash"), ASSET_MODEL_COUNT))
    .withColumn("asset_type_id", (F.pmod(F.col("asset_model_id") - 1, F.lit(ASSET_TYPE_COUNT)) + 1).cast("int"))
    .withColumn("installed_at", random_history_timestamp(F.col("asset_hash")))
)
assets = assets_base.select(
    (F.col("id") + 1).cast("int").alias("asset_id"),
    integer_key(F.col("site_id") - 1, CUSTOMER_COUNT).alias("customer_id"),
    F.col("site_id"), F.col("asset_type_id"), F.col("asset_model_id"),
    F.when((F.col("id") > 0) & (F.pmod(F.col("id"), F.lit(10)) == 0), integer_key(F.floor(F.col("id") / 10), ASSET_COUNT)).cast("int").alias("parent_asset_id"),
    F.format_string("SN-%010d", F.col("id") + 1).alias("serial_number"),
    F.format_string("Asset %08d", F.col("id") + 1).alias("asset_name"),
    F.element_at(F.array(F.lit("low"), F.lit("medium"), F.lit("high"), F.lit("critical")), (F.pmod(F.col("asset_hash"), F.lit(4)) + 1).cast("int")).alias("criticality"),
    F.col("installed_at"),
    (F.col("installed_at") + F.expr("INTERVAL 7 DAYS")).alias("commissioned_at"),
    F.date_add(F.to_date("installed_at"), 730).alias("warranty_end_date"),
    F.lit(None).cast("timestamp").alias("retired_at"),
    F.when(F.pmod(F.col("asset_hash"), F.lit(100)) < 96, "active").when(F.pmod(F.col("asset_hash"), F.lit(100)) < 99, "maintenance").otherwise("offline").alias("operating_status"),
)
record_write("assets", assets, ASSET_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parts, installed components, and sensors

# COMMAND ----------

parts_base = spark.range(PART_COUNT).withColumn("part_hash", stable_hash(F.col("id")))
parts = parts_base.select(
    (F.col("id") + 1).cast("int").alias("part_id"),
    F.format_string("PART-%06d", F.col("id") + 1).alias("part_number"),
    F.format_string("Service Part %04d", F.col("id") + 1).alias("part_name"),
    F.element_at(F.array(*[F.lit(x) for x in ["bearing", "seal", "filter", "belt", "motor", "controller", "battery", "sensor"]]), (F.pmod(F.col("part_hash"), F.lit(8)) + 1).cast("int")).alias("part_category"),
    F.element_at(F.array(F.lit("Northstar"), F.lit("Apex"), F.lit("Vector"), F.lit("Meridian")), (F.pmod(F.col("part_hash"), F.lit(4)) + 1).cast("int")).alias("manufacturer"),
    (25.0 + F.pmod(F.col("part_hash"), F.lit(250_000)) / 100.0).cast("decimal(12,2)").alias("unit_cost"),
    (2_000.0 + F.pmod(F.col("part_hash"), F.lit(50_000))).alias("expected_life_hours"),
    (F.pmod(F.col("part_hash"), F.lit(45)) + 1).cast("int").alias("lead_time_days"),
    (F.pmod(F.col("part_hash"), F.lit(3)) == 0).alias("repairable"),
)
record_write("part_catalog", parts, PART_COUNT)

components_base = spark.range(COMPONENT_COUNT).withColumn("component_hash", stable_hash(F.col("id")))
components = components_base.select(
    (F.col("id") + 1).cast("bigint").alias("asset_component_id"),
    (F.floor(F.col("id") / COMPONENTS_PER_ASSET) + 1).cast("int").alias("asset_id"),
    integer_key(F.col("component_hash"), PART_COUNT).alias("part_id"),
    F.format_string("COMP-%012d", F.col("id") + 1).alias("component_serial_number"),
    F.concat(F.lit("position_"), (F.pmod(F.col("id"), F.lit(COMPONENTS_PER_ASSET)) + 1).cast("string")).alias("component_position"),
    random_history_timestamp(F.col("component_hash")).alias("installed_at"),
    F.lit(None).cast("timestamp").alias("removed_at"),
    (F.pmod(F.col("component_hash"), F.lit(20_000)) / 10.0).alias("installation_meter_hours"),
    F.lit(None).cast("double").alias("removal_meter_hours"),
    F.lit(None).cast("string").alias("removal_reason"),
    F.lit(True).alias("active"),
)
record_write("asset_components", components, COMPONENT_COUNT)

sensor_type_rows = [
    (1, "Temperature", "temperature", "celsius", -20.0, 85.0),
    (2, "Vibration", "rms_vibration", "mm/s", 0.0, 7.1),
    (3, "Current", "electrical_current", "ampere", 0.0, 250.0),
    (4, "Pressure", "pressure", "bar", 0.0, 25.0),
]
assert len(sensor_type_rows) == SENSORS_PER_ASSET
sensor_types = spark.createDataFrame(sensor_type_rows, "sensor_type_id int, sensor_type_name string, measurement_name string, unit string, normal_min double, normal_max double")
record_write("sensor_types", sensor_types, SENSORS_PER_ASSET)

sensors_base = spark.range(SENSOR_COUNT).withColumn("sensor_hash", stable_hash(F.col("id")))
sensors = sensors_base.select(
    (F.col("id") + 1).cast("bigint").alias("sensor_id"),
    (F.floor(F.col("id") / SENSORS_PER_ASSET) + 1).cast("int").alias("asset_id"),
    (F.pmod(F.col("id"), F.lit(SENSORS_PER_ASSET)) + 1).cast("int").alias("sensor_type_id"),
    F.format_string("SENSOR-%012d", F.col("id") + 1).alias("sensor_serial_number"),
    random_history_timestamp(F.col("sensor_hash")).alias("installed_at"),
    random_history_timestamp(F.floor(F.col("sensor_hash") / 3)).alias("last_calibrated_at"),
    F.lit(30).cast("int").alias("sample_interval_seconds"),
    F.lit(True).alias("active"),
)
record_write("sensors", sensors, SENSOR_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Failures and maintenance execution

# COMMAND ----------

failure_modes_base = spark.range(FAILURE_MODE_COUNT)
failure_modes = failure_modes_base.select(
    (F.col("id") + 1).cast("int").alias("failure_mode_id"),
    (F.floor(F.col("id") / FAILURE_MODES_PER_ASSET_TYPE) + 1).cast("int").alias("asset_type_id"),
    F.format_string("FM-%03d", F.col("id") + 1).alias("failure_code"),
    F.concat(F.lit("Failure mode "), (F.pmod(F.col("id"), F.lit(FAILURE_MODES_PER_ASSET_TYPE)) + 1).cast("string")).alias("failure_name"),
    F.element_at(F.array(F.lit("drive"), F.lit("lubrication"), F.lit("electrical"), F.lit("control"), F.lit("structure")), (F.pmod(F.col("id"), F.lit(5)) + 1).cast("int")).alias("subsystem"),
    F.element_at(F.array(F.lit("minor"), F.lit("major"), F.lit("critical")), (F.pmod(F.col("id"), F.lit(3)) + 1).cast("int")).alias("default_severity"),
    F.lit("Synthetic failure classification").alias("description"),
)
record_write("failure_modes", failure_modes, FAILURE_MODE_COUNT)

failures_base = (
    spark.range(FAILURE_COUNT)
    .withColumn("failure_hash", stable_hash(F.col("id"), F.lit("failure")))
    .withColumn("asset_id", integer_key(F.col("failure_hash"), ASSET_COUNT))
    .withColumn("asset_model_id", integer_key(stable_hash(F.col("asset_id") - 1), ASSET_MODEL_COUNT))
    .withColumn("asset_type_id", (F.pmod(F.col("asset_model_id") - 1, F.lit(ASSET_TYPE_COUNT)) + 1).cast("int"))
    .withColumn("detected_at", random_history_timestamp(F.col("failure_hash")))
    .withColumn("downtime_minutes", (15.0 + F.pmod(F.floor(F.col("failure_hash") / 11), F.lit(2_865))).cast("double"))
)
failures = failures_base.select(
    F.col("asset_id"),
    (((F.col("asset_id") - 1) * COMPONENTS_PER_ASSET) + F.pmod(F.col("failure_hash"), F.lit(COMPONENTS_PER_ASSET)) + 1).cast("bigint").alias("asset_component_id"),
    (((F.col("asset_type_id") - 1) * FAILURE_MODES_PER_ASSET_TYPE) + F.pmod(F.floor(F.col("failure_hash") / 7), F.lit(FAILURE_MODES_PER_ASSET_TYPE)) + 1).cast("int").alias("failure_mode_id"),
    F.col("detected_at"), (F.col("detected_at") - F.expr("INTERVAL 5 MINUTES")).alias("failure_started_at"),
    (F.col("detected_at") + F.expr("INTERVAL 10 MINUTES")).alias("confirmed_at"),
    timestamp_at((F.unix_timestamp("detected_at") - F.lit(HISTORY_START_EPOCH) + F.col("downtime_minutes") * 60).cast("long")).alias("restored_at"),
    F.when(F.col("downtime_minutes") > 1_440, "critical").when(F.col("downtime_minutes") > 240, "major").otherwise("minor").alias("severity"),
    F.element_at(F.array(F.lit("telemetry"), F.lit("operator"), F.lit("inspection")), (F.pmod(F.col("failure_hash"), F.lit(3)) + 1).cast("int")).alias("detection_method"),
    F.element_at(F.array(F.lit("wear"), F.lit("lubrication"), F.lit("overload"), F.lit("contamination"), F.lit("electrical")), (F.pmod(F.col("failure_hash"), F.lit(5)) + 1).cast("int")).alias("root_cause"),
    F.lit(False).alias("planned"), F.col("downtime_minutes"),
    (F.col("downtime_minutes") * (10.0 + F.pmod(F.col("failure_hash"), F.lit(90)))).alias("production_impact"),
)
# `machine_failures` is the canonical event table. `failure_details` augments
# the same composite event key with maintenance-specific attributes.
machine_failures = failures.select(
    F.col("asset_id").alias("machine_id"), F.col("detected_at").alias("failure_time")
)
merge_new_rows(
    "machine_failures", machine_failures, ["machine_id", "failure_time"], FAILURE_COUNT
)
results.append(("machine_failures", FAILURE_COUNT))

# Add deterministic maintenance attributes for collected failure events that
# predate this generator. The original two-column events remain unchanged.
collected_failure_base = (
    spark.table(table_name("machine_failures"))
    .where(F.col("machine_id").isNotNull() & F.col("failure_time").isNotNull())
    .withColumn("failure_hash", stable_hash(F.col("machine_id"), F.col("failure_time")))
    .withColumn("downtime_minutes", (15.0 + F.pmod(F.floor(F.col("failure_hash") / 17), F.lit(2_865))).cast("double"))
)
collected_failure_details = collected_failure_base.select(
    "machine_id", "failure_time",
    (((F.col("machine_id") - 1) * COMPONENTS_PER_ASSET) + F.pmod(F.col("failure_hash"), F.lit(COMPONENTS_PER_ASSET)) + 1).cast("bigint").alias("asset_component_id"),
    (F.pmod(F.floor(F.col("failure_hash") / 5), F.lit(FAILURE_MODE_COUNT)) + 1).cast("int").alias("failure_mode_id"),
    (F.col("failure_time") - F.expr("INTERVAL 5 MINUTES")).alias("failure_started_at"),
    (F.col("failure_time") + F.expr("INTERVAL 10 MINUTES")).alias("confirmed_at"),
    F.timestamp_seconds((F.unix_timestamp("failure_time") + F.col("downtime_minutes") * 60).cast("long")).alias("restored_at"),
    F.when(F.col("downtime_minutes") > 1_440, "critical").when(F.col("downtime_minutes") > 240, "major").otherwise("minor").alias("severity"),
    F.lit("historical_record").alias("detection_method"),
    F.element_at(F.array(F.lit("wear"), F.lit("overload"), F.lit("lubrication"), F.lit("electrical")), (F.pmod(F.col("failure_hash"), F.lit(4)) + 1).cast("int")).alias("root_cause"),
    F.lit(False).alias("planned"), F.col("downtime_minutes"),
    (F.col("downtime_minutes") * (10.0 + F.pmod(F.col("failure_hash"), F.lit(90)))).alias("production_impact"),
)
merge_new_rows(
    "failure_details", collected_failure_details,
    ["machine_id", "failure_time"], FAILURE_COUNT
)
results.append(("failure_details", FAILURE_COUNT))

work_orders_base = (
    spark.range(WORK_ORDER_COUNT)
    .withColumn("wo_hash", stable_hash(F.col("id"), F.lit("work_order")))
    .withColumn("corrective", F.col("id") < FAILURE_COUNT)
    .withColumn("asset_id", F.when(F.col("corrective"), integer_key(stable_hash(F.col("id"), F.lit("failure")), ASSET_COUNT)).otherwise(integer_key(F.col("wo_hash"), ASSET_COUNT)))
    .withColumn("opened_at", F.when(F.col("corrective"), random_history_timestamp(stable_hash(F.col("id"), F.lit("failure")))).otherwise(random_history_timestamp(F.col("wo_hash"))))
)
work_orders = work_orders_base.select(
    (F.col("id") + 1).cast("bigint").alias("work_order_id"), F.col("asset_id"),
    F.when(F.col("corrective"), F.col("asset_id")).cast("int").alias("failure_machine_id"),
    F.when(F.col("corrective"), F.col("opened_at")).cast("timestamp").alias("failure_time"),
    F.when(F.col("corrective"), "corrective").otherwise("preventive").alias("work_order_type"),
    F.when(F.col("corrective"), "high").otherwise("normal").alias("priority"), F.lit("completed").alias("status"),
    F.col("opened_at"), (F.col("opened_at") + F.expr("INTERVAL 1 HOUR")).alias("scheduled_at"),
    (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("started_at"),
    (F.col("opened_at") + F.expr("INTERVAL 6 HOURS")).alias("completed_at"),
    F.concat(F.lit("team_"), (F.pmod(F.col("wo_hash"), F.lit(100)) + 1).cast("string")).alias("technician_team"),
    (1.0 + F.pmod(F.col("wo_hash"), F.lit(120)) / 10.0).alias("labor_hours"),
    (100.0 + F.pmod(F.col("wo_hash"), F.lit(1_000_000)) / 100.0).cast("decimal(12,2)").alias("total_cost"),
)
record_write("work_orders", work_orders, WORK_ORDER_COUNT)

action_count = WORK_ORDER_COUNT * 2
actions_base = (
    spark.range(action_count)
    .withColumn("action_hash", stable_hash(F.col("id")))
    .withColumn("work_order_zero", F.floor(F.col("id") / 2))
    .withColumn("corrective", F.col("work_order_zero") < FAILURE_COUNT)
    .withColumn("work_order_hash", stable_hash(F.col("work_order_zero"), F.lit("work_order")))
    .withColumn("asset_id", F.when(F.col("corrective"), integer_key(stable_hash(F.col("work_order_zero"), F.lit("failure")), ASSET_COUNT)).otherwise(integer_key(F.col("work_order_hash"), ASSET_COUNT)))
)
actions = actions_base.select(
    (F.col("id") + 1).cast("bigint").alias("maintenance_action_id"),
    (F.floor(F.col("id") / 2) + 1).cast("bigint").alias("work_order_id"),
    (((F.col("asset_id") - 1) * COMPONENTS_PER_ASSET) + F.pmod(F.col("action_hash"), F.lit(COMPONENTS_PER_ASSET)) + 1).cast("bigint").alias("asset_component_id"),
    F.when(F.pmod(F.col("id"), F.lit(2)) == 0, "inspect").otherwise("repair_or_replace").alias("action_type"),
    (F.pmod(F.col("id"), F.lit(2)) + 1).cast("int").alias("action_sequence"),
    random_history_timestamp(F.col("action_hash")).alias("action_started_at"),
    (random_history_timestamp(F.col("action_hash")) + F.expr("INTERVAL 90 MINUTES")).alias("action_completed_at"),
    F.element_at(F.array(F.lit("passed"), F.lit("repaired"), F.lit("replaced")), (F.pmod(F.col("action_hash"), F.lit(3)) + 1).cast("int")).alias("outcome"),
    F.lit("Synthetic maintenance action").alias("notes"),
)
record_write("maintenance_actions", actions, action_count)

work_order_parts_base = spark.range(FAILURE_COUNT).withColumn("part_hash", stable_hash(F.col("id"), F.lit("part_usage")))
work_order_parts = work_order_parts_base.select(
    (F.col("id") + 1).cast("bigint").alias("work_order_part_id"),
    (F.col("id") + 1).cast("bigint").alias("work_order_id"), integer_key(F.col("part_hash"), PART_COUNT).alias("part_id"),
    (F.pmod(F.col("part_hash"), F.lit(3)) + 1).cast("int").alias("quantity"),
    (25.0 + F.pmod(F.col("part_hash"), F.lit(250_000)) / 100.0).cast("decimal(12,2)").alias("unit_cost"),
    F.format_string("COMP-OLD-%012d", F.col("id") + 1).alias("removed_component_serial"),
    F.format_string("COMP-NEW-%012d", F.col("id") + 1).alias("installed_component_serial"),
)
record_write("work_order_parts", work_order_parts, FAILURE_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operating history

# COMMAND ----------

states_base = spark.range(STATE_EVENT_COUNT).withColumn("state_hash", stable_hash(F.col("id")))
state_slot_seconds = max(1, HISTORY_SECONDS // STATE_EVENTS_PER_ASSET)
states = states_base.select(
    (F.col("id") + 1).cast("bigint").alias("asset_state_event_id"),
    (F.floor(F.col("id") / STATE_EVENTS_PER_ASSET) + 1).cast("int").alias("asset_id"),
    timestamp_at(F.pmod(F.col("id"), F.lit(STATE_EVENTS_PER_ASSET)) * state_slot_seconds).alias("state_start"),
    timestamp_at((F.pmod(F.col("id"), F.lit(STATE_EVENTS_PER_ASSET)) + 1) * state_slot_seconds).alias("state_end"),
    F.element_at(F.array(F.lit("running"), F.lit("running"), F.lit("idle"), F.lit("maintenance"), F.lit("offline")), (F.pmod(F.col("state_hash"), F.lit(5)) + 1).cast("int")).alias("operating_state"),
    F.element_at(F.array(F.lit("production"), F.lit("scheduled_stop"), F.lit("repair"), F.lit("standby")), (F.pmod(F.col("state_hash"), F.lit(4)) + 1).cast("int")).alias("reason"),
    (F.pmod(F.col("state_hash"), F.lit(10_001)) / 100.0).alias("load_percentage"),
)
record_write("asset_state_events", states, STATE_EVENT_COUNT)

meters_base = spark.range(METER_READING_COUNT).withColumn("meter_hash", stable_hash(F.col("id")))
meter_slot_seconds = max(1, HISTORY_SECONDS // METER_READINGS_PER_ASSET)
meters = meters_base.select(
    (F.col("id") + 1).cast("bigint").alias("meter_reading_id"),
    (F.floor(F.col("id") / METER_READINGS_PER_ASSET) + 1).cast("int").alias("asset_id"),
    timestamp_at(F.pmod(F.col("id"), F.lit(METER_READINGS_PER_ASSET)) * meter_slot_seconds).alias("reading_time"),
    ((F.pmod(F.col("id"), F.lit(METER_READINGS_PER_ASSET)) + 1) * (HISTORY_DAYS * 18.0 / METER_READINGS_PER_ASSET)).alias("operating_hours"),
    ((F.pmod(F.col("id"), F.lit(METER_READINGS_PER_ASSET)) + 1) * 10_000 + F.pmod(F.col("meter_hash"), F.lit(1_000))).cast("bigint").alias("cycle_count"),
    ((F.pmod(F.col("id"), F.lit(METER_READINGS_PER_ASSET)) + 1) * 500 + F.pmod(F.col("meter_hash"), F.lit(100))).cast("bigint").alias("starts_count"),
)
record_write("meter_readings", meters, METER_READING_COUNT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generation summary

# COMMAND ----------

display(
    spark.createDataFrame(results, "table_name string, generated_rows long")
    .withColumn("fully_qualified_table", F.concat(F.lit(NAMESPACE + "."), F.col("table_name")))
    .orderBy("table_name")
)
