# Build an Asset Health and Maintenance Monitoring Workbook

Create a complete, interactive Sigma workbook named **Asset Health and Maintenance Monitoring** using the Databricks Delta tables in **exalabs.iot**. Build the workbook elements, sources, calculations, filters, and navigation described below. The audience is fleet operations managers, maintenance planners, reliability engineers, and customer support teams.

The workbook must let users answer:

- Which assets belong to a customer, site, equipment type, or model?
- What operating state is recorded for each asset at the selected time, and how fresh is its telemetry?
- Which assets failed, how often, with what severity, and with what recorded downtime?
- Which components are installed in an asset, where is a particular part used, and which parts were consumed during repairs?
- Which work orders and maintenance actions relate to an asset or failure?
- Which assets have elevated model-estimated failure risk, and what recommendation was recorded?

## 1. Source model

Use the connected Databricks source exposing catalog **exalabs**, schema **iot**. Inspect accessible tables, actual column types, value domains, and date coverage before building. The following table names and column names are from the supplied DDL; all table names below are qualified by exalabs.iot.

| Table | Intended grain and logical key | Content and workbook purpose |
|---|---|---|
| customers | One customer; customer_id | customer_name, industry, service_tier, active; customer filtering and grouping. |
| sites | One site; site_id | customer_id, site_name, site_type, timezone, latitude, longitude, active; customer locations and optional site map. |
| asset_types | One equipment type; asset_type_id | asset_type_name, asset_category, description; equipment filtering. |
| asset_models | One model; asset_model_id | asset_type_id, manufacturer, model_number, rated_capacity, capacity_unit, expected_life_hours, maintenance_interval_hours. |
| assets | One asset; asset_id | customer_id, site_id, asset_type_id, asset_model_id, parent_asset_id, serial_number, asset_name, criticality, installed_at, commissioned_at, warranty_end_date, retired_at, operating_status. |
| part_catalog | One catalog part; part_id | part_number, part_name, part_category, manufacturer, unit_cost, expected_life_hours, lead_time_days, repairable. |
| asset_components | One component installation record; asset_component_id | asset_id, part_id, component_serial_number, component_position, installed_at, removed_at, installation_meter_hours, removal_meter_hours, removal_reason, active. |
| sensor_types | One sensor type; sensor_type_id | sensor_type_name, measurement_name, unit, normal_min, normal_max; measurement metadata. |
| sensors | One registered sensor; sensor_id | asset_id, sensor_type_id, sensor_serial_number, installed_at, last_calibrated_at, sample_interval_seconds, active. |
| iot_sample_data | One telemetry observation; no declared unique row key | machine_id, customer_id, site_id, serial_number, event_time, temperature, cpu_load, network_latency, firmware_version, hardware_configuration_id, rack_id, room_id, latitude, longitude. |
| machine_failures | One canonical failure event; (machine_id, failure_time) | Authoritative source for recorded failure counts. |
| failure_details | One enrichment record per failure; (machine_id, failure_time) | asset_component_id, failure_mode_id, failure_started_at, confirmed_at, restored_at, severity, detection_method, root_cause, planned, downtime_minutes, production_impact. |
| failure_modes | One failure mode; failure_mode_id | asset_type_id, failure_code, failure_name, subsystem, default_severity, description. |
| work_orders | One work order; work_order_id | asset_id, failure_machine_id, failure_time, work_order_type, priority, status, opened_at, scheduled_at, started_at, completed_at, technician_team, labor_hours, total_cost. |
| maintenance_actions | One action; maintenance_action_id | work_order_id, asset_component_id, action_type, action_sequence, action_started_at, action_completed_at, outcome, notes. |
| work_order_parts | One part-usage line; work_order_part_id | work_order_id, part_id, quantity, unit_cost, removed_component_serial, installed_component_serial. |
| asset_state_events | One operating-state interval; asset_state_event_id | asset_id, state_start, state_end, operating_state, reason, load_percentage. |
| meter_readings | One cumulative meter observation; meter_reading_id | asset_id, reading_time, operating_hours, cycle_count, starts_count. |
| asset_feature_snapshots | One feature snapshot; snapshot_id | asset_id, feature_time, window_start, window_end, telemetry_count; temperature, CPU and latency aggregates/spike counts; operating_hours, hours_since_maintenance, failures_last_90d, asset_type_id, asset_model_id, hardware_configuration_id; training labels, forecast_horizon_hours, dataset_split, generated_at. |
| maintenance_predictions | One inference result; prediction_id | asset_id, feature_time, prediction_time, model_name, model_version, forecast_horizon_hours, failure_probability, predicted_failure_mode_id, remaining_useful_life_hours, risk_level, recommended_action, threshold_used, prediction_status, scored_at. |
| maintenance_prediction_outcomes | One intended evaluation per prediction; prediction_id | evaluated_at, horizon_end, failure_observed, actual_machine_id, actual_failure_time, lead_time_hours, outcome_class. |

The supplied generator includes Centrifugal Pump, Air Compressor, Electric Motor, Industrial Fan, Generator, HVAC Unit, Conveyor, and Edge Gateway equipment types. Populate controls from the data rather than hardcoding these examples.

## 2. Relationships and reusable sources

Use these explicit relationships:

- assets.customer_id = customers.customer_id; assets.site_id = sites.site_id; sites.customer_id = customers.customer_id.
- assets.asset_type_id = asset_types.asset_type_id; assets.asset_model_id = asset_models.asset_model_id; asset_models.asset_type_id = asset_types.asset_type_id.
- assets.parent_asset_id references another assets.asset_id. Support parent/child navigation without duplicating child facts into parent totals.
- **iot_sample_data.machine_id and machine_failures.machine_id both map to assets.asset_id.** Use the asset register for consistent customer/site/type filtering and flag conflicting telemetry ownership attributes.
- LEFT JOIN failure_details to machine_failures using **both machine_id and failure_time**. Join failure_details.failure_mode_id to failure_modes; optional asset_component_id links to asset_components and then part_catalog.
- asset_components joins assets by asset_id and part_catalog by part_id. sensors joins assets by asset_id and sensor_types by sensor_type_id.
- work_orders joins assets by asset_id. Its optional failure link is **(failure_machine_id, failure_time) = (machine_id, failure_time)**.
- maintenance_actions and work_order_parts each join work_orders by work_order_id. Actions optionally reference asset_components; usage lines reference part_catalog by part_id.
- State events, meter readings, feature snapshots, and predictions join assets by asset_id. Prediction outcomes join predictions by prediction_id; actual failure references use both actual_machine_id and actual_failure_time.

These are logical keys; the DDL does not enforce primary-key or foreign-key uniqueness. Validate keys and relationship consistency. Deduplicate identical canonical failure keys, but flag conflicting enrichment records rather than silently choosing arbitrary values.

Create a shared **Asset Register** source with one row per asset, plus independent sources for **Asset Snapshot**, **Failure Events**, **State History**, **Telemetry Trends**, **Component Installations**, **Work Orders**, **Maintenance Actions**, **Part Usage**, and **Prediction Evaluation**.

Reduce each latest-record source to at most one row per asset before joining it into Asset Snapshot. Keep one-to-many facts separate or aggregate them to the required grain first. In particular, joining telemetry, failures, components, work-order actions, and part-usage lines into one wide table must not multiply counts, costs, or durations. Use existing governed sources where suitable; otherwise use workbook sources or read-only Databricks SQL.

## 3. Filters and time behavior

Provide visible, searchable controls with clear defaults and a Reset filters action. Use stable IDs as filter values and readable names as labels.

| Control | Behavior |
|---|---|
| Customer | Multi-select customer_name/customer_id; apply to every asset-related page and KPI. |
| Site | Multi-select site_name/site_id; choices narrow to selected customers. |
| Asset type and category | Multi-select asset_type_name and asset_category; applies across the workbook. |
| Manufacturer and asset model | Filter asset_models.manufacturer and model_number; choices narrow to the selected equipment types. |
| Asset search | Search asset_name, asset_id, or serial_number; selection opens asset detail. |
| Criticality and retired assets | Filter asset criticality; exclude assets retired at the selected as-of time by default, with an explicit include-retired option. |
| Reporting period | Explicit start and end timestamps for historical events and trends; use a half-open interval [start, end). |
| As-of timestamp | Controls state, installed components, latest eligible readings, and prediction availability independently of event-period filtering. Default to the reporting end. |
| Operating state and telemetry freshness | Filter the asset snapshot by recorded interval state, including Unknown, and by fresh/stale/missing telemetry. |
| Installed Part | Select part_number/name/category to find distinct assets with that part installed at the as-of time. Apply as a distinct asset-ID filter; several matching components must not duplicate an asset. |
| Consumed Part | Separate part filter on maintenance usage and related work orders, scoped to the reporting period. Label it clearly so it cannot be mistaken for the installed-part filter. |
| Page-specific controls | Failure severity/mode/planned status; work-order status/priority/type/team; model name/version, forecast horizon, and prediction risk level. |

For failure attribution by part, filter through failure_details.asset_component_id to the actual component's part_id. An asset containing a selected part does not prove that the part caused every failure on that asset.

Offer a **Latest available data** reporting default: the last 30 days through the latest nonfuture telemetry/failure timestamp. Advance the exclusive period end by one timestamp tick to include the latest observation, capped at current time. If those sources are empty, use the most recent available operational history. Display that timestamp and each source's date coverage. Offer an explicit **Current time** option. Do not silently shift individual pages to unrelated date windows when a source is older.

Historical filters use failure_time for failures, event_time for telemetry, and opened_at for work-order activity. Use completed_at for completed-order spend and completed-work-order part consumption, labeled as a consumption-date proxy because work_order_parts has no transaction timestamp. Include state intervals that overlap the period and clip them to its boundaries. Show date-basis notes in relevant tooltips. Define as-of fleet membership on an installed-asset basis: exclude assets installed after that instant; label installed but not yet commissioned assets separately. Use commissioned_at as an installation-date fallback only when installed_at is missing. Bound service exposure by commissioned_at, or installed_at when commissioning is unavailable, and retired_at. Retain assets missing both start dates in an Unknown lifecycle date inventory, excluded from historical fleet counts and exposure, and report their count.

Changing the reporting period must not discard an installed component or state interval simply because it began before that period. Keep source timestamps consistent, initially in UTC, and label displayed timezone. Existing data permissions remain in force; a customer dropdown does not establish tenant access restrictions.

## 4. Workbook pages

### Page 1 — Fleet Overview

Create an operational landing page with a compact filter bar, data-as-of banner, KPI row, charts, and a prioritized asset table.

KPIs: distinct assets in scope; assets in each recorded state; assets with stale or missing telemetry; canonical failures in the reporting period; distinct assets with failures; recorded unplanned downtime; and assets with a fresh prediction at or above their stored threshold.

Show assets by operating state and equipment type, a failure trend, and customers/sites ranked by failures and downtime. Add a site-level map using validated site coordinates if useful, with one marker per site and an equivalent table.

The asset table should contain customer, site, asset name/ID/serial, equipment type/model, criticality, registered operating status, as-of operating state, latest telemetry time and age, latest temperature/CPU/latency, latest recorded failure, period failure count, prediction risk/probability, feature timestamp, and recorded recommended action. Sort to surface recorded operating problems and elevated fresh risk. Keep the supporting indicators visible instead of inventing an opaque combined health score.

### Page 2 — Asset Explorer and Detail

Provide a searchable asset register and a detail page or panel opened from an asset row. Preserve the selected asset and customer context during navigation.

Detail sections: identity and customer/site location; parent/child assets; installation, commissioning and warranty dates; registered status versus recorded interval state; latest telemetry and cumulative meter values; temperature, CPU-load, and latency trends; operating-state timeline; recorded failures; installed components and component history; work orders/actions; and prediction history.

Allow navigation from an installed component to its part record, from a failure to linked work orders, and from a work order to its actions and usage lines. Show each detail collection at its own grain.

### Page 3 — Failures and Reliability

Show canonical failures over time, failure severity and mode distribution, affected assets, repeat failures, recorded downtime, and breakdowns by customer, site, type, model, and attributed part. Include an event-level table with failure time, asset, mode, severity, planned/unknown status, component, root cause, restoration time, and recorded downtime.

Include MTTR, observed-state availability, and estimated operating MTBF only when the required data is present and passes duration checks. Show coverage and eligible event counts beside these metrics. Preserve failures without detail records as Unclassified and report enrichment coverage.

### Page 4 — Parts and Components

Provide three connected views: **Installed components**, **Installation/removal history**, and **Maintenance part usage**.

Show part number/name/category/manufacturer, component serial and position, asset/customer/site/type, installation/removal dates, meter values, removal reason, repairability, catalog expected life, and catalog lead time where relevant.

KPIs: distinct installed component records, assets using a selected part, distinct part numbers installed, completed-work-order consumption quantity, known part-usage cost, and failures explicitly attributed to a component of that part. Include top parts by usage and cost, a usage trend, and customer/type breakdowns.

Show component operating age only when a valid as-of asset meter and installation meter support the calculation. Treat lead time and expected life as catalog reference values. Show removed/installed serials from usage transactions as recorded attributes; do not force a serial-number join to the component register without validated matching data.

### Page 5 — Maintenance Operations

Create a work-order table with customer/site/asset, type, priority, status, team, opened/scheduled/started/completed times, labor hours, and recorded total cost. Add status/type breakdowns, completed-maintenance trends, and cost by asset/customer/type.

Show linked failures, ordered maintenance actions, component references, and part usage in work-order detail. An open-backlog or overdue-scheduled-work section may legitimately be empty: the supplied baseline generator writes completed work orders. Overdue scheduled work requires a past scheduled_at and a genuinely unfinished order under a validated status mapping. Do not infer maintenance overdue from unlike clock-hour and operating-hour fields.

### Page 6 — Predictive Risk

Use maintenance_predictions for inference results and asset_feature_snapshots only for feature context. Display model-estimated failure probability, stored risk level, threshold_used, recorded recommended_action, model identity, forecast horizon, feature_time, prediction_time, scored_at, and freshness.

Provide a prioritized risk table, risk distribution, and probability trend for a selected asset. Display the row's forecast_horizon_hours; the supplied training default is 168 hours. Show recommendation text as a model output, with user-friendly labels and the raw value available in detail.

If outcomes are available, add a model-monitoring section with evaluated prediction count, precision, recall, confusion-matrix counts, and lead time for true positives. Filter by model/version/horizon and use one valid outcome per prediction, requiring evaluated_at <= as_of and horizon_end <= as_of. Use prediction_time as the historical prediction-cohort date, not the outcome evaluation date. Missing outcomes mean Pending or Not evaluated, not a negative outcome. Clearly label these as prediction-level evaluation metrics; multiple predictions can reference the same failure.

If predictions or outcomes are empty, show an informative empty state while keeping the operational pages usable.

### Supporting page — Definitions and Data Quality

Document calculation definitions, source date coverage, missing data, proposed threshold settings, validation findings, and optional capabilities unavailable from the current schema. Keep detailed implementation notes here, with concise operational tooltips elsewhere.

## 5. Calculation and interpretation rules

**Operating state.** Select a state interval satisfying state_start <= as_of AND (state_end IS NULL OR state_end > as_of). If more than one interval covers an asset, flag the overlap; a deterministic display choice must not hide it. If no interval covers the timestamp, show Unknown and optionally a separately labeled Last known state. An expired interval is not current state. assets.operating_status is a separate register attribute; the supplied values active/maintenance/offline differ from interval values running/idle/maintenance/offline.

**Latest telemetry and meters.** Filter timestamps to at or before as_of before selecting the latest observation. For meters, order by reading_time then meter_reading_id. Do not use MAX(operating_hours) as a substitute for the latest reading. Telemetry has no unique row ID: inspect equal-timestamp records and use a documented tie rule or aggregate same-time measurements. Show configurable freshness thresholds and distinguish missing readings from zero measurements.

**Failures.** Count distinct (machine_id, failure_time) events from machine_failures. Count affected assets separately. Unplanned means planned = false; null planned or missing details remains Unknown. A past failure does not automatically make the current state Failed. Missing restored_at is an incomplete/unresolved restoration record, not sufficient evidence of current downtime.

**Downtime and MTTR.** Sum nonnegative downtime_minutes once per eligible enriched event and divide by 60. Label this event-based recorded downtime, associated with failures recorded in the reporting period. For MTTR, average recorded downtime hours only over unplanned events restored by the selected as-of time with a valid duration; display the included count. Do not use this event total as calendar-window downtime without validated outage intervals clipped to that window.

**Availability and MTBF.** Clip nonoverlapping state intervals to the reporting window and each asset's eligible service period. Proposed observed-state availability is (running hours + idle hours) / classified observed hours, with maintenance and offline included in the denominator. Show observed coverage against eligible asset-hours; unknown gaps must not become uptime. Estimated operating MTBF is running exposure hours / unplanned failure count for the same asset population and period. Mark low-coverage or inconsistent results as unavailable; return N/A when the failure denominator is zero. Aggregate durations and counts before calculating fleet ratios.

**Parts.** A component is installed at as_of if installed_at <= as_of and removed_at is null or later than as_of. Use timestamps for historical views and flag conflicts with active. Installed count is distinct asset_component_id. Consumption is SUM(quantity) at usage-line grain; known material cost is SUM(quantity * work_order_parts.unit_cost), with missing-price coverage shown. Do not silently replace missing historical prices with part_catalog.unit_cost. Component operating age is a valid as-of operating-hours meter minus installation_meter_hours, or removal_meter_hours minus installation_meter_hours for a removed component; invalid/reset meters mean N/A. A catalog life minus component age is not ML remaining useful life.

**Maintenance cost.** Sum work_orders.total_cost once per order. Present material cost separately because total_cost may already include it. hours_since_maintenance in the training script is elapsed clock time since completed maintenance; maintenance_interval_hours is an operating-hours interval. Do not compare them directly.

**Predictions.** Select one model/version/horizon context before choosing one latest available prediction per asset. Restrict feature_time, prediction_time, and effective scoring time to the selected as-of timestamp; effective scoring time is COALESCE(scored_at, prediction_time). Rank by feature_time DESC, effective scoring time DESC, prediction_time DESC, prediction_id DESC. Flag duplicate IDs and conflicting ties. prediction_status = active alone is insufficient: the script appends predictions and leaves older rows active. Fresh-risk eligibility requires acceptable feature/scoring age and as_of < timestampadd(HOUR, forecast_horizon_hours, feature_time). Scoring historical features again does not renew the trained forecast horizon. Show stale/expired/historical scores separately from fresh risk KPIs. Use stored threshold_used and risk_level; do not assume a fixed 0.5 or 0.65 threshold. Do not mix forecast horizons in a single risk percentage.

The current training script predicts any asset failure; predicted_failure_mode_id and remaining_useful_life_hours are written as null. Show Not available instead of fabricated values. model_version contains an MLflow run ID in the supplied script. The classifier is not demonstrated to be calibrated.

Training labels are anchored to feature_time, while the supplied outcome evaluator anchors horizons to prediction_time. Show both timestamps and flag the difference in the model-monitoring notes, especially for stale features. Treat feature labels as retrospective training data. Stored recommendations are display data; implement monitoring and drill-down interactions without equipment-control actions.

**Available measurements.** Telemetry has temperature, cpu_load, and network_latency. It has no sensor_id, vibration, pressure, or electrical-current measurement. The sensor registry can be displayed, but it must not be joined one-to-many to raw telemetry as if each reading identified an individual sensor. Use documented units and verified measurement mappings; expose any provisional anomaly thresholds as configurable assumptions.

**Parts scope and provenance.** The schema supports component installations, maintenance usage, and part-related failure attribution. Stock-on-hand, suppliers, purchase orders, reorder points, and stockout forecasts require additional data. The supplied generator creates synthetic history; identify this as a demo where that provenance applies. Model results on this data do not establish field predictive performance.

## 6. Reference SQL for failure-event sourcing

Use this Databricks SQL pattern for a failure source. It preserves canonical events with missing enrichment. Before joining, verify assets.asset_id and failure_details(machine_id, failure_time) uniqueness. Add bound reporting-period and asset-scope predicates for production use; this is a source pattern, not an instruction to scan all telemetry.

```sql
WITH failure_keys AS (
  SELECT DISTINCT machine_id, failure_time
  FROM exalabs.iot.machine_failures
  WHERE machine_id IS NOT NULL
    AND failure_time IS NOT NULL
)
SELECT
  f.machine_id AS asset_id,
  f.failure_time,
  a.customer_id,
  a.site_id,
  a.asset_type_id,
  a.asset_model_id,
  d.asset_component_id,
  d.failure_mode_id,
  d.severity,
  d.planned,
  d.restored_at,
  d.downtime_minutes
FROM failure_keys AS f
LEFT JOIN exalabs.iot.assets AS a
  ON a.asset_id = f.machine_id
LEFT JOIN exalabs.iot.failure_details AS d
  ON d.machine_id = f.machine_id
 AND d.failure_time = f.failure_time;
```

## 7. Build quality and acceptance checks

Use a clean operational layout: readable KPI cards, consistent page navigation, restrained colors, and text labels alongside state/risk colors. Use red for critical/offline indications, amber for attention, green for running/low-risk indications, and gray for unknown or missing data; keep observed-state and prediction-risk legends separate. Keep tables searchable, sortable, and exportable where supported.

Push aggregation to Databricks. The source is designed for a large fleet and very large telemetry tables. Filter by asset/customer/time early, aggregate fleet charts by suitable time buckets, and query bounded telemetry for asset detail. Do not download full telemetry into Python or render raw fleet telemetry by default. Reuse sources and calculations. If a database view or refresh optimization is needed but unavailable, document the exact dependency and continue with supported workbook features.

Verify the following before finishing:

1. Customer, site, and asset-type controls change every intended KPI, chart, and detail table consistently; Reset filters restores defaults.
2. Asset Snapshot has one row per asset. Multiple components, work-order actions, usage lines, or predictions do not inflate counts or costs.
3. Failure totals reconcile to distinct canonical failure keys; records without failure_details remain countable. Flag inconsistent asset/customer/site, model/type, failure-mode/type, and component/asset relationships.
4. As-of state and installed components respect interval boundaries. State overlaps, uncovered periods, invalid durations, and meter resets appear in data-quality checks.
5. Installed Part filters find distinct qualifying assets; Consumed Part filters reconcile to usage lines. Part-attributed failure counts use the affected component link.
6. Work-order spend and material usage reconcile independently. Empty backlog, absent prediction outputs, stale data, and missing prices are represented honestly.
7. Risk KPIs use one eligible latest prediction per asset for the selected model/horizon, and prediction metrics exclude unevaluated rows.
8. Selecting an asset, failure, work order, or part opens the correct detail while preserving customer context.

Build the available workbook now, making routine layout choices autonomously. If an essential source or connection is inaccessible, identify it precisely. Finish by reporting the pages and elements created, source mappings, calculation definitions, checks performed, and concrete unresolved dependencies.
