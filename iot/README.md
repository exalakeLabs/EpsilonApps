# IoT predictive-maintenance load model

This project simulates a field telemetry and maintenance application on
Databricks Delta. It provides a connected fleet model, historical maintenance
data, append-only telemetry, operational failures, and prediction results for
load testing and reliability analytics such as MTBF, MTTR, and availability.

## Files

| File | Purpose |
| --- | --- |
| `ddl.sql` | Creates the schema and all 19 telemetry, asset, maintenance, reliability, and prediction tables. |
| `generate_maintenance_data.py` | Creates a consistent historical baseline for the maintenance model. |
| `generate_synthetic_load.py` | Repeatedly appends telemetry and new failure events for load testing. |

## Main entities

- Organization: `customers` and `sites`
- Fleet: `asset_types`, `asset_models`, and `assets`
- Parts: `part_catalog` and `asset_components`
- Instrumentation: `sensor_types` and `sensors`
- Reliability: `machine_failures`, `failure_details`, and `failure_modes`
- Maintenance: `work_orders`, `maintenance_actions`, and `work_order_parts`
- Utilization: `asset_state_events` and `meter_readings`
- Model output: `maintenance_predictions`
- Telemetry: `iot_sample_data`

`iot_sample_data` is the primary measurement fact and retains the collected
temperature, CPU-load, network-latency, firmware, location, and hardware
configuration measures. `machine_failures` is the primary failure-event fact.
`failure_details` augments those events with failure mode, affected component,
downtime, severity, and impact using `(machine_id, failure_time)` as its key.

All generated telemetry `machine_id` values correspond to `assets.asset_id`.
Installed components, sensors, work orders, actions, and predictions also use
keys from the same generated fleet.

## Setup and execution order

1. Confirm that the `exalabs` catalog exists and that the execution identity
   can create schemas and managed Delta tables in it.
2. Run `ddl.sql` in Databricks SQL. It creates the `exalabs.iot` schema and all
   19 Delta tables, including the original telemetry and failure tables.
3. Open `generate_maintenance_data.py` as a Databricks source notebook, attach
   multi-worker Spark compute, review its constants, and run all cells.
4. Open `generate_synthetic_load.py`, configure the desired append workload,
   and run all cells. Run it again whenever another telemetry window and set
   of operational failures is needed.

Run the baseline generator before the append generator. It replaces generated
dimension and maintenance-history tables, but it does not overwrite collected
rows in `iot_sample_data` or `machine_failures`. Synthetic failures are merged
into `machine_failures` by `(machine_id, failure_time)`, and missing detail rows
are added idempotently. The append generator never overwrites data.

## Baseline scale

The default baseline represents:

- 1,000 customers and 5,000 sites
- 100,000 assets across eight equipment types and 40 models
- 600,000 installed components drawn from 500 service parts
- 400,000 sensors across four sensor types
- 250,000 historical failures and 350,000 work orders
- 700,000 maintenance actions and 250,000 part-usage records
- 2 million state intervals and 1 million meter readings
- 1 million predictive-maintenance results

The baseline is deterministic for a given `SEED`. Change the constants at the
top of `generate_maintenance_data.py` to alter fleet size or history volume.
The baseline automatically expands `ASSET_COUNT` when collected telemetry or
failures contain a larger `machine_id`. Keep `MACHINE_COUNT` in the append
generator within that resulting asset range.

## Append workload

The default live workload appends 100 million telemetry rows as 20 Delta
commits, plus 10,000 failures in `machine_failures` with matching rows in
`failure_details`. Fleet attributes remain stable for each machine while
temperature, CPU load, network latency, and timestamps vary by reading. Rare
metric spikes support anomaly and failure-prediction queries.

For a smoke test, use:

```python
TOTAL_TELEMETRY_ROWS = 1_000_000
TELEMETRY_BATCH_ROWS = 100_000
FAILURE_ROWS = 100
OUTPUT_PARTITIONS = 32
```

Smaller telemetry batches create more Delta commits and are useful for testing
transaction-log growth and concurrent readers. Larger batches reduce commit
overhead. Set `OUTPUT_PARTITIONS` in proportion to available Spark cores and
reduce it for small runs to avoid creating many small files.

## Example reliability queries

Operating-hour MTBF by asset model:

```sql
WITH latest_meter AS (
  SELECT asset_id, MAX(operating_hours) AS operating_hours
  FROM exalabs.iot.meter_readings
  GROUP BY asset_id
), failures AS (
  SELECT machine_id AS asset_id, COUNT(*) AS failure_count
  FROM exalabs.iot.machine_failures
  GROUP BY machine_id
)
SELECT
  am.manufacturer,
  am.model_number,
  SUM(m.operating_hours) / NULLIF(SUM(COALESCE(f.failure_count, 0)), 0) AS mtbf_hours,
  SUM(COALESCE(f.failure_count, 0)) AS failures
FROM exalabs.iot.assets AS a
JOIN exalabs.iot.asset_models AS am USING (asset_model_id)
JOIN latest_meter AS m USING (asset_id)
LEFT JOIN failures AS f USING (asset_id)
GROUP BY am.manufacturer, am.model_number
ORDER BY mtbf_hours;
```

MTTR and availability by asset type:

```sql
SELECT
  at.asset_type_name,
  AVG(f.downtime_minutes) / 60.0 AS mttr_hours,
  SUM(f.downtime_minutes) / 60.0 AS total_downtime_hours,
  COUNT(*) AS failure_count
FROM exalabs.iot.failure_details AS f
JOIN exalabs.iot.assets AS a ON f.machine_id = a.asset_id
JOIN exalabs.iot.asset_types AS at ON a.asset_type_id = at.asset_type_id
WHERE f.planned = false
GROUP BY at.asset_type_name
ORDER BY failure_count DESC;
```

Prediction outcome analysis:

```sql
SELECT
  risk_level,
  COUNT(*) AS predictions,
  COUNT(actual_failure_time) AS observed_failures,
  COUNT(actual_failure_time) / COUNT(*) AS observed_failure_rate,
  AVG(failure_probability) AS mean_predicted_probability
FROM exalabs.iot.maintenance_predictions
GROUP BY risk_level
ORDER BY mean_predicted_probability DESC;
```

The synthetic data is intended for engineering and demonstration workloads.
It should not be used to validate the statistical performance of a real
predictive-maintenance model.
