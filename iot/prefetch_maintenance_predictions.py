# Databricks notebook source
"""Prefetch gamma classifications into a one-row-per-asset Delta cache.

Run on Databricks with DATABRICKS_TOKEN supplied through the compute environment.
The complete cache is replaced only after every endpoint batch succeeds.
"""
import math
import os
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql import Window, functions as F

NAMESPACE = "exalabs.iot"
ENDPOINT_URL = "https://dbc-72732cb5-7d02.cloud.databricks.com/serving-endpoints/gamma/invocations"
CACHE_TABLE = f"{NAMESPACE}.asset_maintenance_prediction_cache"
BATCH_SIZE = 256
FORECAST_HORIZON_HOURS = 168
FEATURE_COLUMNS = [
    "telemetry_count", "temperature_mean", "temperature_stddev", "temperature_min",
    "temperature_max", "temperature_slope", "cpu_mean", "cpu_p95", "cpu_max",
    "latency_mean", "latency_p95", "latency_max", "temperature_spike_count",
    "cpu_spike_count", "latency_spike_count", "operating_hours",
    "hours_since_maintenance", "failures_last_90d", "asset_type_id",
    "asset_model_id", "hardware_configuration_id",
]


def score_batch(session, rows):
    # Match training-time numeric cleanup; never include identifiers as features.
    values = []
    for row in rows:
        cleaned = []
        for name in FEATURE_COLUMNS:
            value = float(row[name]) if row[name] is not None else 0.0
            cleaned.append(value if math.isfinite(value) else 0.0)
        values.append(cleaned)
    response = session.post(ENDPOINT_URL, json={
        "dataframe_split": {"columns": FEATURE_COLUMNS, "data": values}
    }, timeout=(10, 120))
    if response.status_code != 200:
        # Do not include response bodies or request headers in shared notebook logs.
        raise RuntimeError(f"gamma invocation failed: HTTP {response.status_code}")
    predictions = response.json().get("predictions")
    if not isinstance(predictions, list) or len(predictions) != len(rows):
        raise ValueError("gamma must return one prediction per input row")
    for prediction in predictions:
        if not isinstance(prediction, dict):
            raise ValueError("gamma returned class labels only; deploy the serving wrapper from train_maintenance_model.py")
        probability = prediction.get("failure_probability")
        threshold = prediction.get("threshold_used")
        for name, value in [("failure_probability", probability), ("threshold_used", threshold)]:
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 1:
                raise ValueError(f"Invalid {name} in gamma response")
        if prediction.get("predicted_failure") not in (0, 1) or prediction["predicted_failure"] != int(probability >= threshold):
            raise ValueError("Invalid or inconsistent predicted_failure in gamma response")
        if prediction.get("risk_level") not in ("low", "medium", "high", "critical"):
            raise ValueError("Invalid risk_level in gamma response")
        if not isinstance(prediction.get("recommended_action"), str):
            raise ValueError("Missing recommended_action in gamma response")
    return predictions


def prefetch(spark):
    token = os.environ.get("DATABRICKS_TOKEN")
    if not token:
        raise RuntimeError("Set DATABRICKS_TOKEN in the Databricks compute environment using a secret reference")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    assets = spark.table(f"{NAMESPACE}.assets").select("asset_id").cache()
    if assets.where("asset_id IS NULL").take(1) or assets.groupBy("asset_id").count().where("count > 1").take(1):
        raise ValueError("assets.asset_id must be unique and non-null")
    count = assets.count()
    if not count:
        raise ValueError("Asset register is empty; existing cache was preserved")
    as_of = datetime.utcnow()
    features = spark.table(f"{NAMESPACE}.asset_feature_snapshots").where(F.col("feature_time") <= F.lit(as_of))
    # Conflicting latest snapshots must not silently change the model inputs.
    latest = features.withColumn("rank", F.dense_rank().over(
        Window.partitionBy("asset_id").orderBy(F.col("feature_time").desc())
    )).where("rank = 1").select("asset_id", "feature_time", *FEATURE_COLUMNS).distinct()
    if latest.groupBy("asset_id").count().where("count > 1").take(1):
        raise ValueError("Conflicting latest feature snapshots; resolve duplicates before scoring")
    source = assets.join(latest, "asset_id", "left")
    records, batch = [], []
    with requests.Session() as session:
        session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        session.mount("https://", HTTPAdapter(max_retries=Retry(
            total=4, backoff_factor=1, status_forcelist=[429, 502, 503, 504],
            allowed_methods=["POST"], respect_retry_after_header=True,
        )))

        def flush():
            if not batch:
                return
            predictions = score_batch(session, batch)
            scored_at = datetime.utcnow()
            for row, prediction in zip(batch, predictions):
                status = "expired" if row.feature_time + timedelta(hours=FORECAST_HORIZON_HOURS) <= scored_at else "scored"
                records.append((int(row.asset_id), row.feature_time, scored_at, "gamma",
                    FORECAST_HORIZON_HOURS, float(prediction["failure_probability"]),
                    int(prediction["predicted_failure"]), prediction["risk_level"],
                    prediction["recommended_action"], float(prediction["threshold_used"]), status))
            batch.clear()

        for row in source.toLocalIterator():
            if row.feature_time is None:
                records.append((int(row.asset_id), None, as_of, "gamma", FORECAST_HORIZON_HOURS,
                                None, None, None, None, None, "missing_features"))
            else:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    flush()
        flush()
    if len(records) != count:
        raise RuntimeError("Coverage mismatch; existing cache was preserved")
    schema = "asset_id int, feature_time timestamp, scored_at timestamp, serving_endpoint string, forecast_horizon_hours int, failure_probability double, predicted_failure int, risk_level string, recommended_action string, threshold_used double, prediction_status string"
    result = spark.createDataFrame(records, schema)
    result.write.format("delta").mode("overwrite").saveAsTable(CACHE_TABLE)
    result.groupBy("prediction_status").count().show()
    assets.unpersist()
    print(f"Cached {count} assets in {CACHE_TABLE}")


if __name__ == "__main__":
    prefetch(spark)
