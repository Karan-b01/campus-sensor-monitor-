# Campus Sensor Monitoring and Alert System

A TypeScript backend for ingesting campus sensor readings, buffering bursts in Redis Streams, storing time-series data in TimescaleDB, and evaluating alert rules asynchronously.

## Features

- Accept a sensor reading or a batch of readings at `POST /ingest`.
- Queue readings in Redis Streams and return `202 Accepted` after Redis accepts the batch.
- Persist readings in a TimescaleDB hypertable using bulk inserts.
- Evaluate single-reading and rolling-window alert rules in a background worker.
- List, acknowledge, and resolve alerts through the API.
- Generate a 10,000-reading burst with the TypeScript simulator.

## Requirements

- Node.js 20 or later (required by Fastify 5).
- Docker Desktop with Docker Compose.
- npm.

## Install and run

### 1. Start Redis and TimescaleDB

From the project root:

```bash
docker compose up -d
docker compose ps
```

Redis is published on `localhost:6379`. TimescaleDB is published on `localhost:5433` and creates the `campus_sensors` database.

### 2. Install dependencies and create the database schema

```bash
npm install
npm run migrate
```

The migration creates the sensor readings hypertable, alert rules table, and alerts table. It is safe to run again.

### 3. Start the API and worker

Open separate terminals in the project root.

**Terminal 1 — API:**

```bash
npm run dev
```

The API listens on `http://localhost:3000`.

**Terminal 2 — worker:**

```bash
npm run worker
```

Run one worker instance with the current Redis stream polling implementation.

### 4. Run the simulator (optional)

With the API and worker running, open a third terminal:

```bash
npm run simulator
```

The simulator sends one JSON batch containing 10,000 readings to `/ingest`. It prints the HTTP status, queued count, and time until the API response. Running it again sends another batch and creates more database rows and alerts.

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/ingest` | Queue one reading or an array of readings |
| `GET` | `/rules` | List alert rules |
| `POST` | `/rules` | Create an alert rule |
| `PATCH` | `/rules/:id/toggle` | Enable or disable a rule with `{ "enabled": true }` |
| `GET` | `/alerts` | List alerts with their rule details |
| `PATCH` | `/alerts/:id/acknowledge` | Acknowledge an open alert |
| `PATCH` | `/alerts/:id/resolve` | Resolve an alert |

Example reading:

```json
{
  "sensorId": "library-floor-2",
  "type": "temperature",
  "value": 34.5,
  "timestamp": "2026-10-12T10:30:00Z"
}
```

For example, with PowerShell:

```powershell
$reading = @{
  sensorId = "library-floor-2"
  type = "temperature"
  value = 34.5
  timestamp = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:3000/ingest" -Method Post -ContentType "application/json" -Body $reading
```

`202 Accepted` means the reading is queued; it does not mean database persistence or alert evaluation has completed. Check the worker terminal and query `GET /alerts` to inspect generated alerts.

## Load-test report

The TypeScript simulator sends a single batch of 10,000 readings. In one local run, the API returned `202 Accepted` with `count: 10000` in **124.80 ms**. This measures API acceptance and queuing time, not end-to-end worker completion time.

After a follow-up run with unique millisecond timestamps, the Redis stream drained to zero and the database contained 10,000 additional unique readings. The worker generated matching alerts for readings that met the enabled rules. This was a local development run; hardware details and repeated benchmark statistics were not recorded, so the result is an observation rather than a performance guarantee.

Run the TypeScript check with:

```bash
npm run typecheck
```

There is no automated unit-test suite or k6 scenario configured yet.

## Architecture and trade-offs

### Fastify API and Redis Streams

The API adds each submitted reading to the `sensor_stream` Redis stream in a Redis `MULTI`/`EXEC` batch, then responds with `202`. This keeps database writes off the request path and allows the API to accept a burst before the worker catches up.

The trade-off is eventual consistency: queued readings are not immediately queryable in TimescaleDB. The current Compose file does not configure Redis replication or an explicit persistence volume, so this setup does not promise that queued data will survive every Redis or host failure. The ingestion route also relies on the submitted reading shape rather than performing full schema validation.

### TimescaleDB persistence

`sensor_readings` is a hypertable partitioned by time. The worker uses the `pg` driver to insert each batch in one SQL statement, with a maximum batch size of 1,000. The primary key `(time, sensor_id, metric_type)` prevents duplicate readings at the same timestamp; conflicting inserts are ignored so the worker can safely retry persistence.

Rolling-window rules use SQL aggregation over readings in the requested time range. This keeps aggregation in the database, though query cost depends on data volume and the number of active window rules.

### Background worker

The worker polls up to 1,000 stream entries at a time, validates their fields, writes readings, evaluates enabled rules, stores any generated alerts, and removes successfully processed entries. Keeping this work out of the HTTP request helps avoid tying API response time to database insert and rule-evaluation time.

The current worker uses stream range reads and deletes rather than Redis consumer groups. Run a single worker instance; multiple workers would need coordination to avoid processing the same entries concurrently. The worker is also a single serial consumer, so its processing capacity limits how quickly the queue drains.

## Local configuration

Database connection settings can be supplied with `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME`. The default credentials in `docker-compose.yml` are for local development only; do not use them in a deployed environment.

Stop the local data services with:

```bash
docker compose down
```

To also delete the persisted TimescaleDB volume and all local database data:

```bash
docker compose down -v
```
