CREATE SCHEMA IF NOT EXISTS exalabs.iot;

CREATE TABLE IF NOT EXISTS exalabs.iot.iot_sample_data (
  customer_id INT COMMENT 'Identifier for the customer who owns or operates the IoT device.',
  site_id INT COMMENT 'Unique identifier for the physical location or site where the device is installed.',
  machine_id INT COMMENT 'Identifier representing the specific machine or device emitting the IoT data.',
  serial_number STRING COLLATE UTF8_BINARY COMMENT 'Manufacturer-assigned serial number uniquely identifying the device.',
  firmware_version STRING COLLATE UTF8_BINARY COMMENT 'Version number of the firmware currently running on the device.',
  latitude DOUBLE COMMENT 'Geographical latitude coordinate of the device\'s installation location.',
  longitude DOUBLE COMMENT 'Geographical longitude coordinate of the device\'s installation location.',
  temperature DOUBLE COMMENT 'Recorded temperature measurement from the device or its environment.',
  cpu_load DOUBLE COMMENT 'Current CPU utilization percentage of the device indicating processing activity.',
  network_latency DOUBLE COMMENT 'Measured network latency in milliseconds affecting device communication.',
  rack_id INT COMMENT 'Identifier for the rack within the site where the device is mounted.',
  room_id INT COMMENT 'Identifier designating the specific room at the site where the device is located.',
  hardware_configuration_id INT COMMENT 'Reference ID for the hardware setup or configuration of the device.',
  event_time TIMESTAMP COMMENT 'Timestamp indicating when the data measurement or event was recorded.')
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0');

-- Asset hierarchy, component, maintenance, reliability, and prediction model.

CREATE TABLE IF NOT EXISTS exalabs.iot.customers (
  customer_id INT NOT NULL,
  customer_name STRING NOT NULL,
  industry STRING,
  service_tier STRING,
  active BOOLEAN,
  created_at TIMESTAMP)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.sites (
  site_id INT NOT NULL,
  customer_id INT NOT NULL,
  site_name STRING NOT NULL,
  site_type STRING,
  timezone STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  commissioned_at TIMESTAMP,
  active BOOLEAN)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.asset_types (
  asset_type_id INT NOT NULL,
  asset_type_name STRING NOT NULL,
  asset_category STRING,
  description STRING)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.asset_models (
  asset_model_id INT NOT NULL,
  asset_type_id INT NOT NULL,
  manufacturer STRING NOT NULL,
  model_number STRING NOT NULL,
  rated_capacity DOUBLE,
  capacity_unit STRING,
  expected_life_hours DOUBLE,
  maintenance_interval_hours DOUBLE)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.assets (
  asset_id INT NOT NULL,
  customer_id INT NOT NULL,
  site_id INT NOT NULL,
  asset_type_id INT NOT NULL,
  asset_model_id INT NOT NULL,
  parent_asset_id INT,
  serial_number STRING NOT NULL,
  asset_name STRING NOT NULL,
  criticality STRING,
  installed_at TIMESTAMP,
  commissioned_at TIMESTAMP,
  warranty_end_date DATE,
  retired_at TIMESTAMP,
  operating_status STRING)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.part_catalog (
  part_id INT NOT NULL,
  part_number STRING NOT NULL,
  part_name STRING NOT NULL,
  part_category STRING,
  manufacturer STRING,
  unit_cost DECIMAL(12,2),
  expected_life_hours DOUBLE,
  lead_time_days INT,
  repairable BOOLEAN)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.asset_components (
  asset_component_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  part_id INT NOT NULL,
  component_serial_number STRING,
  component_position STRING,
  installed_at TIMESTAMP NOT NULL,
  removed_at TIMESTAMP,
  installation_meter_hours DOUBLE,
  removal_meter_hours DOUBLE,
  removal_reason STRING,
  active BOOLEAN)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.sensor_types (
  sensor_type_id INT NOT NULL,
  sensor_type_name STRING NOT NULL,
  measurement_name STRING NOT NULL,
  unit STRING NOT NULL,
  normal_min DOUBLE,
  normal_max DOUBLE)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.sensors (
  sensor_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  sensor_type_id INT NOT NULL,
  sensor_serial_number STRING NOT NULL,
  installed_at TIMESTAMP,
  last_calibrated_at TIMESTAMP,
  sample_interval_seconds INT,
  active BOOLEAN)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.failure_modes (
  failure_mode_id INT NOT NULL,
  asset_type_id INT NOT NULL,
  failure_code STRING NOT NULL,
  failure_name STRING NOT NULL,
  subsystem STRING,
  default_severity STRING,
  description STRING)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.failure_details (
  machine_id INT NOT NULL,
  failure_time TIMESTAMP NOT NULL,
  asset_component_id BIGINT,
  failure_mode_id INT NOT NULL,
  failure_started_at TIMESTAMP,
  confirmed_at TIMESTAMP,
  restored_at TIMESTAMP,
  severity STRING,
  detection_method STRING,
  root_cause STRING,
  planned BOOLEAN,
  downtime_minutes DOUBLE,
  production_impact DOUBLE)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.work_orders (
  work_order_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  failure_machine_id INT,
  failure_time TIMESTAMP,
  work_order_type STRING NOT NULL,
  priority STRING,
  status STRING,
  opened_at TIMESTAMP NOT NULL,
  scheduled_at TIMESTAMP,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  technician_team STRING,
  labor_hours DOUBLE,
  total_cost DECIMAL(12,2))
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.maintenance_actions (
  maintenance_action_id BIGINT NOT NULL,
  work_order_id BIGINT NOT NULL,
  asset_component_id BIGINT,
  action_type STRING NOT NULL,
  action_sequence INT,
  action_started_at TIMESTAMP,
  action_completed_at TIMESTAMP,
  outcome STRING,
  notes STRING)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.work_order_parts (
  work_order_part_id BIGINT NOT NULL,
  work_order_id BIGINT NOT NULL,
  part_id INT NOT NULL,
  quantity INT NOT NULL,
  unit_cost DECIMAL(12,2),
  removed_component_serial STRING,
  installed_component_serial STRING)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.asset_state_events (
  asset_state_event_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  state_start TIMESTAMP NOT NULL,
  state_end TIMESTAMP,
  operating_state STRING NOT NULL,
  reason STRING,
  load_percentage DOUBLE)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.meter_readings (
  meter_reading_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  reading_time TIMESTAMP NOT NULL,
  operating_hours DOUBLE,
  cycle_count BIGINT,
  starts_count BIGINT)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.maintenance_predictions (
  prediction_id BIGINT NOT NULL,
  asset_id INT NOT NULL,
  feature_time TIMESTAMP NOT NULL,
  prediction_time TIMESTAMP NOT NULL,
  model_name STRING NOT NULL,
  model_version STRING NOT NULL,
  forecast_horizon_hours INT NOT NULL,
  failure_probability DOUBLE NOT NULL,
  predicted_failure_mode_id INT,
  remaining_useful_life_hours DOUBLE,
  risk_level STRING,
  recommended_action STRING,
  threshold_used DOUBLE,
  prediction_status STRING,
  scored_at TIMESTAMP)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.asset_feature_snapshots (
  snapshot_id STRING NOT NULL,
  asset_id INT NOT NULL,
  feature_time TIMESTAMP NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end TIMESTAMP NOT NULL,
  telemetry_count BIGINT,
  temperature_mean DOUBLE,
  temperature_stddev DOUBLE,
  temperature_min DOUBLE,
  temperature_max DOUBLE,
  temperature_slope DOUBLE,
  cpu_mean DOUBLE,
  cpu_p95 DOUBLE,
  cpu_max DOUBLE,
  latency_mean DOUBLE,
  latency_p95 DOUBLE,
  latency_max DOUBLE,
  temperature_spike_count BIGINT,
  cpu_spike_count BIGINT,
  latency_spike_count BIGINT,
  operating_hours DOUBLE,
  hours_since_maintenance DOUBLE,
  failures_last_90d BIGINT,
  asset_type_id INT,
  asset_model_id INT,
  hardware_configuration_id INT,
  label_failure_within_horizon INT,
  label_failure_time TIMESTAMP,
  forecast_horizon_hours INT,
  dataset_split STRING,
  generated_at TIMESTAMP)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.maintenance_prediction_outcomes (
  prediction_id BIGINT NOT NULL,
  evaluated_at TIMESTAMP NOT NULL,
  horizon_end TIMESTAMP NOT NULL,
  failure_observed BOOLEAN NOT NULL,
  actual_machine_id INT,
  actual_failure_time TIMESTAMP,
  lead_time_hours DOUBLE,
  outcome_class STRING NOT NULL)
USING DELTA;

CREATE TABLE IF NOT EXISTS exalabs.iot.machine_failures (
  machine_id INT,
  failure_time TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'delta.parquet.format.version' = '2.12.0',
  'delta.parquet.format.version.afe.internal' = '2.12.0');
