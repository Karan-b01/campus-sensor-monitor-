# Campus Sensor Monitoring and Alert System

A TypeScript backend for ingesting campus sensor readings, buffering bursts in Redis Streams, storing time-series data in TimescaleDB, and evaluating alert rules asynchronously.

## Features

- Accept a sensor reading or a batch of readings at `POST /ingest`.
- Validate required reading fields, timestamps, finite values, and a maximum batch of 10,000.
- Queue readings in Redis Streams and return `202 Accepted` after Redis accepts the batch.
- Persist readings in a TimescaleDB hypertable using bulk inserts.
- Process each batch in a database transaction and safely retry stream entries.
- Evaluate single-reading and rolling-window alert rules in a background worker.
- Keep at most one unresolved alert for each rule and sensor; resolving it allows a later recurrence to alert again.
- Create, edit, enable, and disable alert rules through the API.
- List, acknowledge, and resolve alerts through the API.
- Generate a 10,000-reading burst with the TypeScript simulator.

## Project structure

```text
src/
  api/          Fastify rule and alert routes
  config/       Redis and PostgreSQL clients
  db/           SQL schema and migration runner
  domain/       Alert-rule operators and evaluation
  lib/          Reading validation and batch parsing
  services/     Redis stream worker
  types/        Shared TypeScript data types
  simulator.ts  Local 10,000-reading load simulator
```

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

Redis is published on `localhost:6379` with AOF persistence on a named volume. TimescaleDB is published on `localhost:5433` and creates the `campus_sensors` database. Both services have health checks.

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
| `PATCH` | `/rules/:id` | Edit an alert rule |
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

The TypeScript simulator sends a single batch of 10,000 readings. In the latest local run, the API returned `202 Accepted` with `count: 10000` in **131.21 ms**. This measures API acceptance and queuing time, not end-to-end worker completion time.

After a follow-up run with unique millisecond timestamps, the Redis stream drained to zero and the database contained 10,000 additional unique readings. The worker generated matching alerts for readings that met the enabled rules. This was a local development run; hardware details and repeated benchmark statistics were not recorded, so the result is an observation rather than a performance guarantee.

Run the TypeScript check with:

```bash
npm run typecheck
npm test
```

The tests cover reading validation and all supported threshold operators. There is no k6 scenario configured yet.

## Architecture and trade-offs

### Fastify API and Redis Streams

The API validates each submitted reading and adds the accepted readings to the `sensor_stream` Redis stream in a Redis `MULTI`/`EXEC` batch, then responds with `202`. Requests may contain up to 10,000 readings and the JSON body limit is 5 MiB. This keeps database writes off the request path and allows the API to accept a burst before the worker catches up.

The trade-off is eventual consistency: queued readings are not immediately queryable in TimescaleDB. Redis AOF persistence reduces queue loss on a container restart, but it is not replication or a backup and an `everysec` fsync policy can still lose a small amount of recent data during a host failure.

### TimescaleDB persistence

`sensor_readings` is a hypertable partitioned by time. The worker uses the `pg` driver to insert each batch in one SQL statement, with a maximum batch size of 1,000. The primary key `(time, sensor_id, metric_type)` prevents duplicate readings at the same timestamp; conflicting inserts are ignored so the worker can safely retry persistence.

Rolling-window rules use SQL aggregation over readings in the requested time range. This keeps aggregation in the database, though query cost depends on data volume and the number of active window rules.

### Background worker

The worker polls up to 1,000 stream entries at a time, validates their fields, and writes readings and generated alerts in one database transaction. It removes stream entries only after that transaction commits. If removal fails, a retry encounters the reading primary key and does not evaluate it again, preventing duplicate single-reading alerts. A unique alert deduplication key prevents repeated unresolved alerts for the same rule and sensor; resolving an alert clears that key so a later recurrence can create a new alert.

Window rules are evaluated only for sensors that had new readings in the committed batch. This avoids repeatedly scanning every sensor and emitting the same alert for unrelated batches.

When a batch fails, the worker keeps its valid stream entries for retry and uses capped exponential backoff to avoid flooding logs or the database during an outage. Malformed stream entries are logged and discarded because they cannot be persisted.

The current worker uses stream range reads and deletes rather than Redis consumer groups. Run a single worker instance; multiple workers would need coordination to avoid processing the same entries concurrently. The worker is also a single serial consumer, so its processing capacity limits how quickly the queue drains.

## Local configuration

Database connection settings can be supplied with `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME`. Set `REDIS_URL` to change the Redis connection string. The default credentials in `docker-compose.yml` are for local development only; do not use them in a deployed environment.

Stop the local data services with:

```bash
docker compose down
```

To also delete the persisted TimescaleDB data and Redis queue:

```bash
docker compose down -v
```
