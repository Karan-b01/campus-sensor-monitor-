CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS sensor_readings (
  time        TIMESTAMPTZ NOT NULL,
  sensor_id   TEXT NOT NULL,
  metric_type TEXT NOT NULL,
  value       DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (time, sensor_id, metric_type)
);

SELECT create_hypertable('sensor_readings', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS sensor_readings_sensor_time_idx
  ON sensor_readings (sensor_id, time DESC);

CREATE TABLE IF NOT EXISTS alert_rules (
  id             BIGSERIAL PRIMARY KEY,
  name           TEXT NOT NULL,
  sensor_type    TEXT NOT NULL,
  operator       TEXT NOT NULL CHECK (operator IN ('>', '>=', '<', '<=', '=', '!=')),
  threshold      DOUBLE PRECISION NOT NULL,
  window_minutes INTEGER NOT NULL DEFAULT 0 CHECK (window_minutes >= 0),
  enabled        BOOLEAN NOT NULL DEFAULT TRUE,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
