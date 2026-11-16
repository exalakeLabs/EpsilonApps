# Databricks notebook source
# MAGIC %md
# MAGIC # Dexcom CGM ingest -> `exalabs.dexcom.egv_readings`
# MAGIC Incrementally pulls estimated glucose values (EGVs) from the Dexcom API and MERGEs them into Unity Catalog.
# MAGIC Schedule it with the job in `dexcom_ingest_job.json` (every 30 minutes).
# MAGIC
# MAGIC **One-time setup** (Databricks CLI): create a secret scope `dexcom` with keys `client_id`, `client_secret`
# MAGIC and `refresh_token` (the refresh token from the first OAuth authorization; only used to bootstrap).
# MAGIC After the first run the rotating refresh token lives in `exalabs.dexcom.oauth_state`.

# COMMAND ----------

dbutils.widgets.text("catalog", "exalabs")
dbutils.widgets.text("schema", "dexcom")
dbutils.widgets.text("secret_scope", "dexcom")
dbutils.widgets.dropdown("environment", "sandbox", ["sandbox", "production"])
dbutils.widgets.text("backfill_days", "30", "Backfill days (first run only)")
dbutils.widgets.text("overlap_hours", "6", "Re-read overlap (hours)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SCOPE = dbutils.widgets.get("secret_scope")
BASE = {"sandbox": "https://sandbox-api.dexcom.com", "production": "https://api.dexcom.com"}[dbutils.widgets.get("environment")]
BACKFILL_DAYS = int(dbutils.widgets.get("backfill_days"))
OVERLAP_HOURS = int(dbutils.widgets.get("overlap_hours"))
TABLE = f"{CATALOG}.{SCHEMA}.egv_readings"
STATE = f"{CATALOG}.{SCHEMA}.oauth_state"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
  record_id STRING, user_id STRING, system_time TIMESTAMP, display_time TIMESTAMP_NTZ,
  glucose_mgdl INT, trend STRING, trend_rate DOUBLE, unit STRING, rate_unit STRING,
  transmitter_generation STRING, display_device STRING, display_app STRING, ingested_at TIMESTAMP)""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {STATE} (refresh_token STRING, updated_at TIMESTAMP)")

# COMMAND ----------

import json, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

def http(url, data=None, headers=None):
    req = urllib.request.Request(url, data=data, headers=headers or {})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)

def get_access_token():
    """Exchange the stored refresh token for an access token and persist the rotated refresh token."""
    row = spark.sql(f"SELECT refresh_token FROM {STATE} ORDER BY updated_at DESC LIMIT 1").collect()
    refresh = row[0]["refresh_token"] if row else dbutils.secrets.get(SCOPE, "refresh_token")
    body = urllib.parse.urlencode({
        "client_id": dbutils.secrets.get(SCOPE, "client_id"),
        "client_secret": dbutils.secrets.get(SCOPE, "client_secret"),
        "refresh_token": refresh,
        "grant_type": "refresh_token",
    }).encode()
    tok = http(BASE + "/v3/oauth2/token", body, {"Content-Type": "application/x-www-form-urlencoded"})
    if tok.get("refresh_token"):
        spark.createDataFrame([(tok["refresh_token"], datetime.now(timezone.utc))], "refresh_token string, updated_at timestamp") \
            .write.mode("append").saveAsTable(STATE)
    return tok["access_token"]

access = get_access_token()
auth = {"Authorization": "Bearer " + access}

# COMMAND ----------

fmt = lambda d: d.strftime("%Y-%m-%dT%H:%M:%S")
parse = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)

rng = http(BASE + "/v3/users/self/dataRange", headers=auth)
data_end = parse(rng["egvs"]["end"]["systemTime"]) + timedelta(seconds=1)

last = spark.sql(f"SELECT max(system_time) AS m FROM {TABLE}").collect()[0]["m"]
start = (last.replace(tzinfo=None) - timedelta(hours=OVERLAP_HOURS)) if last else data_end - timedelta(days=BACKFILL_DAYS)
print("window:", start, "->", data_end)

# Dexcom allows at most 30 days per request
records, cursor = [], start
while cursor < data_end:
    chunk_end = min(cursor + timedelta(days=30), data_end)
    q = urllib.parse.urlencode({"startDate": fmt(cursor), "endDate": fmt(chunk_end)})
    resp = http(BASE + "/v3/users/self/egvs?" + q, headers=auth)
    records += resp.get("records", resp.get("egvs", []))
    user_id = resp.get("userId")
    cursor = chunk_end
print("fetched", len(records), "records")

# COMMAND ----------

if records:
    rows = [(
        r["recordId"], user_id, parse(r["systemTime"]), datetime.fromisoformat(r["displayTime"][:19]),
        r.get("value"), r.get("trend"), r.get("trendRate"), r.get("unit"), r.get("rateUnit"),
        r.get("transmitterGeneration"), r.get("displayDevice"), r.get("displayApp"),
    ) for r in records if r.get("value") is not None]
    schema = ("record_id string, user_id string, system_time timestamp, display_time timestamp_ntz, glucose_mgdl int, "
              "trend string, trend_rate double, unit string, rate_unit string, transmitter_generation string, "
              "display_device string, display_app string")
    spark.createDataFrame(rows, schema).createOrReplaceTempView("egv_incoming")
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING (SELECT *, current_timestamp() AS ingested_at FROM egv_incoming) s
        ON t.record_id = s.record_id
        WHEN NOT MATCHED THEN INSERT *""")

display(spark.sql(f"SELECT count(*) AS readings, min(system_time) AS first_reading, max(system_time) AS last_reading FROM {TABLE}"))
