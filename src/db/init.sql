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

CREATE TABLE IF NOT EXISTS alerts (
  id               BIGSERIAL PRIMARY KEY,
  rule_id          BIGINT NOT NULL REFERENCES alert_rules(id),
  sensor_id        TEXT NOT NULL,
  current_value    DOUBLE PRECISION NOT NULL,
  message          TEXT NOT NULL,
  triggered_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  status           TEXT NOT NULL DEFAULT 'OPEN'
                   CHECK (status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')),
  acknowledged_at  TIMESTAMPTZ,
  resolved_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS alerts_triggered_at_idx
  ON alerts (triggered_at DESC);
