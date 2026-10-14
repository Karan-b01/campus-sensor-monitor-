# Campus Sensor Monitoring and Alerts

A backend service for collecting campus sensor readings, evaluating alert rules, and helping organizers manage alerts. The system is being built to handle bursty sensor traffic and time-window rules such as average temperature over the previous ten minutes.

> **Project status:** early development. The repository currently contains the Docker Compose definition and a TypeScript project scaffold. The API, stream processing, persistence, rule evaluation, simulator, and load tests are planned work and are not implemented yet.

## Project goals

- Accept individual sensor readings and batches.
- Absorb bursts of up to 10,000 readings per second without overwhelming the storage layer.
- Let organizers create, edit, enable, and disable alert rules.
- Evaluate rules against individual readings and rolling time windows.
- Create alerts when a rule matches, and let organizers view, acknowledge, and resolve them.
- Provide a sensor simulator and a reproducible load-test report.

Example reading:

```json
{
  "sensorId": "library-floor-2",
  "type": "temperature",
  "value": 34.5,
  "timestamp": "2026-10-12T10:30:00Z"
}
```

## Planned technology

| Component | Technology | Purpose |
| --- | --- | --- |
| API | Fastify + TypeScript | Typed HTTP ingestion and organizer endpoints |
| Burst buffer | Redis Streams | Queue readings during traffic spikes for asynchronous processing |
| Storage and analytics | TimescaleDB | Persist time-series readings and support rolling-window queries |
| Load testing | k6 | Exercise burst traffic and produce performance results for the submission report |
| Sensor simulator | Python `asyncio` + `httpx` | Generate realistic campus readings and temperature trends |

## Current repository

```text
.
├── docker-compose.yml       # Local TimescaleDB and Redis services
├── src/
│   ├── server.ts            # API entry point (planned)
│   └── config/
│       ├── database.ts      # Database connection (planned)
│       └── redis.ts         # Redis connection (planned)
├── package.json             # Node dependencies
└── tsconfig.json             # TypeScript compiler settings
```

## Getting started

### Prerequisites

- Docker Desktop with Docker Compose
- Node.js and npm
- Python 3 for the planned simulator
- k6 for the planned load tests

### Start the local data services

From the repository root:

```sh
docker compose up -d
docker compose ps
```

The Compose file publishes TimescaleDB on `localhost:5432` and Redis on `localhost:6379`. The database is named `campus_sensors`; its current local development credentials are configured in `docker-compose.yml`.

Stop the services with:

```sh
docker compose down
```

To remove the persisted local database volume as well:

```sh
docker compose down -v
```

### API and tooling

The API and supporting scripts are not implemented yet, so there is currently no start, simulator, or load-test command. The Node dependencies can be installed with:

```sh
npm install
```

## Architecture plan

1. Fastify validates incoming readings and batches, then appends accepted events to a Redis Stream.
2. A worker consumes stream entries, persists readings in TimescaleDB, and evaluates configured rules.
3. Matching rules create alerts that organizers can query and manage through the API.
4. The Python simulator generates repeatable campus sensor patterns; k6 measures ingestion throughput, latency, and error rate under burst load.

Redis buffering is intended to smooth short-lived bursts. It does not by itself guarantee that every offered request can be accepted indefinitely: stream retention, consumer capacity, backpressure, and failure recovery will need explicit limits and monitoring.

## Planned API surface

The endpoint names and request schemas below are design targets and may change during implementation.

| Area | Planned operations |
| --- | --- |
| Readings | Submit a reading or batch; query readings by sensor, type, and time range |
| Rules | Create, list, update, enable, and disable alert rules |
| Alerts | List, acknowledge, and resolve generated alerts |
| Health | Report API, Redis, and database readiness |

## Performance reporting

The submission target is a repeatable burst test of 10,000 readings in one second. The final report should include the test environment, offered and accepted throughput, latency percentiles, errors or dropped readings, and the behavior of the stream and database after the burst. No benchmark results are claimed yet; they will be added after the load test is implemented and run.

## Development status

- [x] Local Redis and TimescaleDB services defined with Docker Compose
- [ ] Fastify server and health endpoint
- [ ] Reading ingestion and batch validation
- [ ] Redis Stream producer and consumer worker
- [ ] TimescaleDB schema and migrations
- [ ] Rule management and rolling-window evaluation
- [ ] Alert lifecycle endpoints
- [ ] Python sensor simulator
- [ ] k6 burst scenario and performance report

## Configuration and security

The current Compose file uses development-only database credentials. Before sharing or deploying this project, move credentials into environment variables and rotate any credentials that have been exposed. Do not use the current values in a production environment.

## License

No license has been selected yet.
