# Databricks notebook source
# MAGIC %md
# MAGIC # Train and score an IoT maintenance model
# MAGIC
# MAGIC Builds point-in-time feature snapshots from `iot_sample_data`, labels them
# MAGIC from `machine_failures`, trains an XGBoost classifier, logs and registers
# MAGIC the model with MLflow, writes batch predictions, and evaluates predictions
# MAGIC whose forecast horizons have closed.

# COMMAND ----------

import math
from datetime import datetime, timedelta, timezone

import mlflow
import mlflow.xgboost
import numpy as np
import pandas as pd
import xgboost as xgb
from mlflow.models import infer_signature
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Target model and tables.
CATALOG = "exalabs"
SCHEMA = "iot"
REGISTERED_MODEL_NAME = f"{CATALOG}.{SCHEMA}.maintenance_failure_xgboost"
EXPERIMENT_NAME = "/Shared/iot-maintenance-xgboost"

# Feature, label, and training configuration.
FEATURE_WINDOW_HOURS = 24
FORECAST_HORIZON_HOURS = 168
FAILURE_HISTORY_DAYS = 90
MAX_LOCAL_TRAINING_ROWS = 1_000_000
RANDOM_SEED = 42
DEFAULT_ALERT_THRESHOLD = 0.65
WRITE_FEATURE_SNAPSHOTS = True
WRITE_BATCH_PREDICTIONS = True
EVALUATE_MATURE_PREDICTIONS = True

# XGBoost parameters. The class imbalance weight is calculated from training data.
XGB_PARAMS = {
    "n_estimators": 600,
    "max_depth": 8,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 5,
    "reg_lambda": 2.0,
    "objective": "binary:logistic",
    "eval_metric": "aucpr",
    "tree_method": "hist",
    "random_state": RANDOM_SEED,
    "n_jobs": -1,
    "early_stopping_rounds": 40,
}

NAMESPACE = f"{CATALOG}.{SCHEMA}"
FEATURE_TABLE = f"{NAMESPACE}.asset_feature_snapshots"
PREDICTION_TABLE = f"{NAMESPACE}.maintenance_predictions"
OUTCOME_TABLE = f"{NAMESPACE}.maintenance_prediction_outcomes"

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate inputs

# COMMAND ----------

REQUIRED_TABLES = [
    "iot_sample_data", "machine_failures", "assets", "meter_readings",
    "work_orders", "asset_feature_snapshots", "maintenance_predictions",
    "maintenance_prediction_outcomes",
]
missing = [name for name in REQUIRED_TABLES if not spark.catalog.tableExists(f"{NAMESPACE}.{name}")]
if missing:
    raise RuntimeError(f"Missing tables: {', '.join(missing)}. Run iot/ddl.sql first.")

if FEATURE_WINDOW_HOURS <= 0 or FORECAST_HORIZON_HOURS <= 0:
    raise ValueError("Feature and forecast windows must be positive")

FEATURE_COLUMNS = [
    "telemetry_count",
    "temperature_mean",
    "temperature_stddev",
    "temperature_min",
    "temperature_max",
    "temperature_slope",
    "cpu_mean",
    "cpu_p95",
    "cpu_max",
    "latency_mean",
    "latency_p95",
    "latency_max",
    "temperature_spike_count",
    "cpu_spike_count",
    "latency_spike_count",
    "operating_hours",
    "hours_since_maintenance",
    "failures_last_90d",
    "asset_type_id",
    "asset_model_id",
    "hardware_configuration_id",
]
LABEL_COLUMN = "label_failure_within_horizon"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build point-in-time feature snapshots

# COMMAND ----------

telemetry = (
    spark.table(f"{NAMESPACE}.iot_sample_data")
    .where(F.col("machine_id").isNotNull() & F.col("event_time").isNotNull())
)
failures = (
    spark.table(f"{NAMESPACE}.machine_failures")
    .where(F.col("machine_id").isNotNull() & F.col("failure_time").isNotNull())
    .dropDuplicates(["machine_id", "failure_time"])
)

if telemetry.limit(1).count() == 0:
    raise RuntimeError("iot_sample_data is empty; load telemetry before training")

windowed = telemetry.withColumn(
    "feature_window", F.window("event_time", f"{FEATURE_WINDOW_HOURS} hours")
)

telemetry_features = (
    windowed.groupBy("machine_id", "feature_window")
    .agg(
        F.count(F.lit(1)).cast("bigint").alias("telemetry_count"),
        F.avg("temperature").alias("temperature_mean"),
        F.stddev_samp("temperature").alias("temperature_stddev"),
        F.min("temperature").alias("temperature_min"),
        F.max("temperature").alias("temperature_max"),
        F.min_by("temperature", "event_time").alias("temperature_first"),
        F.max_by("temperature", "event_time").alias("temperature_last"),
        F.avg("cpu_load").alias("cpu_mean"),
        F.percentile_approx("cpu_load", 0.95, 10_000).alias("cpu_p95"),
        F.max("cpu_load").alias("cpu_max"),
        F.avg("network_latency").alias("latency_mean"),
        F.percentile_approx("network_latency", 0.95, 10_000).alias("latency_p95"),
        F.max("network_latency").alias("latency_max"),
        F.sum(F.when(F.col("temperature") > 70.0, 1).otherwise(0)).cast("bigint").alias("temperature_spike_count"),
        F.sum(F.when(F.col("cpu_load") > 90.0, 1).otherwise(0)).cast("bigint").alias("cpu_spike_count"),
        F.sum(F.when(F.col("network_latency") > 150.0, 1).otherwise(0)).cast("bigint").alias("latency_spike_count"),
        F.max_by("hardware_configuration_id", "event_time").cast("int").alias("hardware_configuration_id"),
    )
    .select(
        F.col("machine_id").cast("int").alias("asset_id"),
        F.col("feature_window.start").alias("window_start"),
        F.col("feature_window.end").alias("window_end"),
        F.col("feature_window.end").alias("feature_time"),
        "telemetry_count", "temperature_mean", "temperature_stddev",
        "temperature_min", "temperature_max",
        ((F.col("temperature_last") - F.col("temperature_first")) / FEATURE_WINDOW_HOURS).alias("temperature_slope"),
        "cpu_mean", "cpu_p95", "cpu_max", "latency_mean", "latency_p95",
        "latency_max", "temperature_spike_count", "cpu_spike_count",
        "latency_spike_count", "hardware_configuration_id",
    )
)

snapshot_keys = telemetry_features.select("asset_id", "feature_time").dropDuplicates()

# Point-in-time operating hours.
meter_features = (
    snapshot_keys.alias("s")
    .join(
        spark.table(f"{NAMESPACE}.meter_readings").alias("m"),
        (F.col("s.asset_id") == F.col("m.asset_id"))
        & (F.col("m.reading_time") <= F.col("s.feature_time")),
        "left",
    )
    .groupBy(F.col("s.asset_id").alias("asset_id"), F.col("s.feature_time").alias("feature_time"))
    .agg(F.max_by("m.operating_hours", "m.reading_time").alias("operating_hours"))
)

# Point-in-time time since completed maintenance.
maintenance_features = (
    snapshot_keys.alias("s")
    .join(
        spark.table(f"{NAMESPACE}.work_orders").where(F.col("status") == "completed").alias("w"),
        (F.col("s.asset_id") == F.col("w.asset_id"))
        & (F.col("w.completed_at") <= F.col("s.feature_time")),
        "left",
    )
    .groupBy(F.col("s.asset_id").alias("asset_id"), F.col("s.feature_time").alias("feature_time"))
    .agg(F.max("w.completed_at").alias("last_maintenance_time"))
    .withColumn(
        "hours_since_maintenance",
        (F.unix_timestamp("feature_time") - F.unix_timestamp("last_maintenance_time")) / 3_600.0,
    )
    .drop("last_maintenance_time")
)

# Historical failures use only events before feature_time.
failure_history = (
    snapshot_keys.alias("s")
    .join(
        failures.alias("f"),
        (F.col("s.asset_id") == F.col("f.machine_id"))
        & (F.col("f.failure_time") < F.col("s.feature_time"))
        & (F.col("f.failure_time") >= F.col("s.feature_time") - F.expr(f"INTERVAL {FAILURE_HISTORY_DAYS} DAYS")),
        "left",
    )
    .groupBy(F.col("s.asset_id").alias("asset_id"), F.col("s.feature_time").alias("feature_time"))
    .agg(F.count("f.failure_time").cast("bigint").alias("failures_last_90d"))
)

# Labels look strictly forward from feature_time.
future_failures = (
    snapshot_keys.alias("s")
    .join(
        failures.alias("f"),
        (F.col("s.asset_id") == F.col("f.machine_id"))
        & (F.col("f.failure_time") > F.col("s.feature_time"))
        & (F.col("f.failure_time") <= F.col("s.feature_time") + F.expr(f"INTERVAL {FORECAST_HORIZON_HOURS} HOURS")),
        "left",
    )
    .groupBy(F.col("s.asset_id").alias("asset_id"), F.col("s.feature_time").alias("feature_time"))
    .agg(F.min("f.failure_time").alias("label_failure_time"))
)

data_cutoff = (
    telemetry.select(F.max("event_time").alias("event_time"))
    .crossJoin(failures.select(F.max("failure_time").alias("failure_time")))
    .select(F.greatest("event_time", "failure_time").alias("data_cutoff"))
    .first()["data_cutoff"]
)

features = (
    telemetry_features
    .join(spark.table(f"{NAMESPACE}.assets").select("asset_id", "asset_type_id", "asset_model_id"), "asset_id", "left")
    .join(meter_features, ["asset_id", "feature_time"], "left")
    .join(maintenance_features, ["asset_id", "feature_time"], "left")
    .join(failure_history, ["asset_id", "feature_time"], "left")
    .join(future_failures, ["asset_id", "feature_time"], "left")
    .withColumn(
        LABEL_COLUMN,
        F.when(
            F.col("feature_time") <= F.lit(data_cutoff) - F.expr(f"INTERVAL {FORECAST_HORIZON_HOURS} HOURS"),
            F.when(F.col("label_failure_time").isNotNull(), 1).otherwise(0),
        ).cast("int"),
    )
)

feature_diagnostics = features.agg(
    F.min("feature_time").alias("earliest_feature_time"),
    F.max("feature_time").alias("latest_feature_time"),
    F.countDistinct("feature_time").alias("feature_window_count"),
    F.count(F.when(F.col(LABEL_COLUMN).isNotNull(), 1)).alias("closed_snapshot_count"),
).first()

labelled_bounds = (
    features.where(F.col(LABEL_COLUMN).isNotNull())
    .agg(
        F.min(F.unix_timestamp("feature_time")).alias("min_epoch"),
        F.max(F.unix_timestamp("feature_time")).alias("max_epoch"),
    )
    .first()
)
if labelled_bounds["min_epoch"] is None or labelled_bounds["max_epoch"] <= labelled_bounds["min_epoch"]:
    latest_closed_feature_time = data_cutoff - timedelta(hours=FORECAST_HORIZON_HOURS)
    raise RuntimeError(
        "Insufficient closed historical feature windows. "
        f"Observed feature times: {feature_diagnostics['earliest_feature_time']} through "
        f"{feature_diagnostics['latest_feature_time']} "
        f"({feature_diagnostics['feature_window_count']} distinct windows; "
        f"{feature_diagnostics['closed_snapshot_count']} closed snapshots). "
        f"Data cutoff: {data_cutoff}; a {FORECAST_HORIZON_HOURS}-hour forecast "
        f"requires feature_time <= {latest_closed_feature_time}. "
        "Load telemetry and failures spanning more than the forecast horizon. "
        "For synthetic data, rerun generate_synthetic_load.py with "
        "EVENT_TIME_SPAN_DAYS greater than FORECAST_HORIZON_HOURS / 24."
    )

time_span = labelled_bounds["max_epoch"] - labelled_bounds["min_epoch"]
train_boundary = labelled_bounds["min_epoch"] + int(time_span * 0.70)
validation_boundary = labelled_bounds["min_epoch"] + int(time_span * 0.85)

feature_snapshots = features.select(
    F.sha2(F.concat_ws("|", F.col("asset_id"), F.col("feature_time").cast("string")), 256).alias("snapshot_id"),
    "asset_id", "feature_time", "window_start", "window_end", "telemetry_count",
    "temperature_mean", "temperature_stddev", "temperature_min", "temperature_max",
    "temperature_slope", "cpu_mean", "cpu_p95", "cpu_max", "latency_mean",
    "latency_p95", "latency_max", "temperature_spike_count", "cpu_spike_count",
    "latency_spike_count", "operating_hours", "hours_since_maintenance",
    "failures_last_90d", "asset_type_id", "asset_model_id",
    "hardware_configuration_id", F.col(LABEL_COLUMN), "label_failure_time",
    F.lit(FORECAST_HORIZON_HOURS).cast("int").alias("forecast_horizon_hours"),
    F.when(F.col(LABEL_COLUMN).isNull(), "inference")
    .when(F.unix_timestamp("feature_time") <= train_boundary, "train")
    .when(F.unix_timestamp("feature_time") <= validation_boundary, "validation")
    .otherwise("test").alias("dataset_split"),
    F.current_timestamp().alias("generated_at"),
)

if WRITE_FEATURE_SNAPSHOTS:
    expected_columns = spark.table(FEATURE_TABLE).columns
    if feature_snapshots.columns != expected_columns:
        raise RuntimeError(f"Feature schema mismatch: {feature_snapshots.columns} != {expected_columns}")
    feature_snapshots.write.mode("overwrite").insertInto(FEATURE_TABLE)

display(feature_snapshots.groupBy("dataset_split", LABEL_COLUMN).count().orderBy("dataset_split", LABEL_COLUMN))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create chronological local training sets

# COMMAND ----------

training_source = spark.table(FEATURE_TABLE).where(F.col(LABEL_COLUMN).isNotNull())
split_limits = {
    "train": int(MAX_LOCAL_TRAINING_ROWS * 0.70),
    "validation": int(MAX_LOCAL_TRAINING_ROWS * 0.15),
    "test": int(MAX_LOCAL_TRAINING_ROWS * 0.15),
}


def collect_split(split_name):
    return (
        training_source.where(F.col("dataset_split") == split_name)
        .select(*FEATURE_COLUMNS, LABEL_COLUMN)
        .orderBy(F.rand(RANDOM_SEED))
        .limit(split_limits[split_name])
        .toPandas()
    )


train_pdf = collect_split("train")
validation_pdf = collect_split("validation")
test_pdf = collect_split("test")

for split_name, frame in [("train", train_pdf), ("validation", validation_pdf), ("test", test_pdf)]:
    if frame.empty or frame[LABEL_COLUMN].nunique() < 2:
        raise RuntimeError(f"{split_name} must contain both positive and negative labels")
    frame[FEATURE_COLUMNS] = frame[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).fillna(0.0)

X_train, y_train = train_pdf[FEATURE_COLUMNS], train_pdf[LABEL_COLUMN]
X_validation, y_validation = validation_pdf[FEATURE_COLUMNS], validation_pdf[LABEL_COLUMN]
X_test, y_test = test_pdf[FEATURE_COLUMNS], test_pdf[LABEL_COLUMN]

positive_count = int(y_train.sum())
negative_count = len(y_train) - positive_count
scale_pos_weight = negative_count / max(1, positive_count)
print(f"Training rows={len(y_train):,}, positives={positive_count:,}, scale_pos_weight={scale_pos_weight:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train, evaluate, and register XGBoost

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)
mlflow.xgboost.autolog(log_models=False, log_datasets=True, silent=True)

model = xgb.XGBClassifier(**XGB_PARAMS, scale_pos_weight=scale_pos_weight)

with mlflow.start_run(run_name="maintenance-failure-168h-xgboost") as active_run:
    model.fit(
        X_train,
        y_train,
        eval_set=[(X_train, y_train), (X_validation, y_validation)],
        verbose=False,
    )

    validation_probability = model.predict_proba(X_validation)[:, 1]
    precision_curve, recall_curve, thresholds = precision_recall_curve(y_validation, validation_probability)
    if len(thresholds):
        f1 = 2 * precision_curve[:-1] * recall_curve[:-1] / np.maximum(precision_curve[:-1] + recall_curve[:-1], 1e-12)
        alert_threshold = float(thresholds[int(np.nanargmax(f1))])
    else:
        alert_threshold = DEFAULT_ALERT_THRESHOLD

    test_probability = model.predict_proba(X_test)[:, 1]
    test_prediction = (test_probability >= alert_threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_test, test_prediction, labels=[0, 1]).ravel()
    metrics = {
        "test_roc_auc": roc_auc_score(y_test, test_probability),
        "test_pr_auc": average_precision_score(y_test, test_probability),
        "test_precision": precision_score(y_test, test_prediction, zero_division=0),
        "test_recall": recall_score(y_test, test_prediction, zero_division=0),
        "test_brier_score": brier_score_loss(y_test, test_probability),
        "alert_threshold": alert_threshold,
        "test_true_positive": int(tp),
        "test_false_positive": int(fp),
        "test_true_negative": int(tn),
        "test_false_negative": int(fn),
    }
    mlflow.log_metrics(metrics)
    mlflow.log_params({
        "forecast_horizon_hours": FORECAST_HORIZON_HOURS,
        "feature_window_hours": FEATURE_WINDOW_HOURS,
        "failure_history_days": FAILURE_HISTORY_DAYS,
        "feature_count": len(FEATURE_COLUMNS),
    })

    signature = infer_signature(X_train.head(100), model.predict_proba(X_train.head(100))[:, 1])
    model_info = mlflow.xgboost.log_model(
        xgb_model=model,
        artifact_path="model",
        registered_model_name=REGISTERED_MODEL_NAME,
        signature=signature,
        input_example=X_train.head(5),
        model_format="json",
    )
    run_id = active_run.info.run_id

print(f"Registered {REGISTERED_MODEL_NAME} from MLflow run {run_id}")
display(spark.createDataFrame([(name, float(value)) for name, value in metrics.items()], "metric string, value double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch inference for the latest asset snapshots

# COMMAND ----------

if WRITE_BATCH_PREDICTIONS:
    latest_window = Window.partitionBy("asset_id").orderBy(F.col("feature_time").desc())
    latest_features = (
        spark.table(FEATURE_TABLE)
        .withColumn("row_number", F.row_number().over(latest_window))
        .where(F.col("row_number") == 1)
        .drop("row_number")
        .select("asset_id", "feature_time", *FEATURE_COLUMNS)
    )

    scoring_schema = "asset_id int, feature_time timestamp, failure_probability double"

    def score_partitions(iterator):
        for batch in iterator:
            model_input = batch[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            yield pd.DataFrame({
                "asset_id": batch["asset_id"].astype("int32"),
                "feature_time": batch["feature_time"],
                "failure_probability": model.predict_proba(model_input)[:, 1].astype("float64"),
            })

    scored = latest_features.mapInPandas(score_partitions, scoring_schema)
    predictions = scored.select(
        F.xxhash64("asset_id", "feature_time", F.lit(run_id)).cast("bigint").alias("prediction_id"),
        "asset_id", "feature_time", F.current_timestamp().alias("prediction_time"),
        F.lit(REGISTERED_MODEL_NAME).alias("model_name"), F.lit(run_id).alias("model_version"),
        F.lit(FORECAST_HORIZON_HOURS).cast("int").alias("forecast_horizon_hours"),
        "failure_probability", F.lit(None).cast("int").alias("predicted_failure_mode_id"),
        F.lit(None).cast("double").alias("remaining_useful_life_hours"),
        F.when(F.col("failure_probability") >= max(0.85, alert_threshold), "critical")
        .when(F.col("failure_probability") >= alert_threshold, "high")
        .when(F.col("failure_probability") >= alert_threshold * 0.5, "medium")
        .otherwise("low").alias("risk_level"),
        F.when(F.col("failure_probability") >= max(0.85, alert_threshold), "stop_and_inspect")
        .when(F.col("failure_probability") >= alert_threshold, "schedule_maintenance")
        .otherwise("continue_monitoring").alias("recommended_action"),
        F.lit(alert_threshold).cast("double").alias("threshold_used"),
        F.lit("active").alias("prediction_status"), F.current_timestamp().alias("scored_at"),
    )
    if predictions.columns != spark.table(PREDICTION_TABLE).columns:
        raise RuntimeError("Prediction output does not match maintenance_predictions DDL")
    predictions.write.mode("append").insertInto(PREDICTION_TABLE)
    print(f"Wrote batch predictions from MLflow run {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate predictions whose horizons have closed

# COMMAND ----------

if EVALUATE_MATURE_PREDICTIONS:
    existing_outcomes = spark.table(OUTCOME_TABLE).select("prediction_id")
    matured = (
        spark.table(PREDICTION_TABLE)
        .withColumn(
            "horizon_end",
            F.timestamp_seconds(
                (F.unix_timestamp("prediction_time") + F.col("forecast_horizon_hours") * 3_600).cast("long")
            ),
        )
        .where(F.col("horizon_end") <= F.current_timestamp())
        .join(existing_outcomes, "prediction_id", "left_anti")
    )
    outcome_candidates = (
        matured.alias("p")
        .join(
            failures.alias("f"),
            (F.col("p.asset_id") == F.col("f.machine_id"))
            & (F.col("f.failure_time") > F.col("p.prediction_time"))
            & (F.col("f.failure_time") <= F.col("p.horizon_end")),
            "left",
        )
        .groupBy(
            "p.prediction_id", "p.asset_id", "p.prediction_time", "p.horizon_end",
            "p.failure_probability", "p.threshold_used",
        )
        .agg(F.min("f.failure_time").alias("actual_failure_time"))
    )
    outcomes = outcome_candidates.select(
        "prediction_id", F.current_timestamp().alias("evaluated_at"), "horizon_end",
        F.col("actual_failure_time").isNotNull().alias("failure_observed"),
        F.when(F.col("actual_failure_time").isNotNull(), F.col("asset_id")).cast("int").alias("actual_machine_id"),
        "actual_failure_time",
        F.when(
            F.col("actual_failure_time").isNotNull(),
            (F.unix_timestamp("actual_failure_time") - F.unix_timestamp("prediction_time")) / 3_600.0,
        ).alias("lead_time_hours"),
        F.when((F.col("failure_probability") >= F.col("threshold_used")) & F.col("actual_failure_time").isNotNull(), "true_positive")
        .when((F.col("failure_probability") >= F.col("threshold_used")) & F.col("actual_failure_time").isNull(), "false_positive")
        .when((F.col("failure_probability") < F.col("threshold_used")) & F.col("actual_failure_time").isNotNull(), "false_negative")
        .otherwise("true_negative").alias("outcome_class"),
    )
    if outcomes.columns != spark.table(OUTCOME_TABLE).columns:
        raise RuntimeError("Outcome output does not match maintenance_prediction_outcomes DDL")
    outcomes.write.mode("append").insertInto(OUTCOME_TABLE)
    display(outcomes.groupBy("outcome_class").count())
