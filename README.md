# Gorder

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![CI](https://github.com/venexene/gorder/actions/workflows/ci.yml/badge.svg)](https://github.com/venexene/gorder/actions)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Event-driven order processing service. Kafka ingestion, PostgreSQL persistence, in-memory LRU cache, REST API, JWT auth with RBAC, rate limiting, Swagger docs, Prometheus and Grafana metrics, structured logging. Built with Go.

## Tech Stack

**Go** · **Gin** · **PostgreSQL** · **Apache Kafka** · **Docker** · **Prometheus** · **Grafana** · **JWT** · **Swagger** · **GitHub Actions**

## Quick start

```
cp .env.example .env
make up
```

Open `http://localhost:8080/login` — sign in as `admin` / `admin`. Swagger UI at `/swagger/index.html`. The emulator sends test orders from `testdata/` to Kafka automatically on first run.

## API

Swagger UI available at `/swagger/index.html` when the service is running.

### Public endpoints

```
GET  /health/live          server liveness
GET  /health/ready         database and Kafka connectivity
GET  /metrics              Prometheus metrics
GET  /swagger/*any         Swagger UI
GET  /api/version          build version and commit hash
GET  /login                login page (HTML)
POST /login                authenticate, sets httpOnly cookies  [rate-limited]
POST /logout               clear cookies
GET  /register             registration page (HTML)
POST /register             create new user                      [rate-limited]
POST /refresh              get new access token from refresh    [rate-limited]
```

### User endpoints (JWT required, role: user or admin)

```
GET  /                           all orders (HTML)
GET  /:uid                       single order (HTML)
GET  /api/user/orders/:uid       order by UID
GET  /api/user/all_orders_uids   all user's order UIDs
```

### Admin endpoints (JWT required, role: admin)

```
GET  /api/admin/orders/:uid      order by UID
GET  /api/admin/all_orders_uids  all order UIDs
```

## Authentication

- Passwords hashed with bcrypt (cost 10)
- Tokens stored in httpOnly cookies for browser or `Authorization: Bearer <token>` header for API
- Access token: 15 minutes, refresh token: 7 days with rotation
- Browsers are redirected to `/login` when unauthenticated
- Role-based access: `admin` sees all orders, `user` sees only their own
- Default users created by migration: `admin`/`admin`, `alice`/`alice`, `bob`/`bob`

## Rate Limiting

Auth endpoints are rate-limited to prevent brute-force attacks:

| Endpoint | Default limit | Config variable |
|----------|--------------|-----------------|
| `POST /login` | 5 req/sec | `RATE_LIMIT` |
| `POST /register` | 3 req/min | `RATE_LIMIT_REGISTER` |
| `POST /refresh` | 5 req/sec | `RATE_LIMIT` |

Format: `rate-period` where period is `S` - second, `M` - minute, or `H` - hour. Uses `ulule/limiter` with in-memory store.

## Flow

Write path:

```
Kafka -> consumer -> validate -> PostgreSQL
                              -> LRU cache
```

Read path:

```
GET /api/orders/:uid -> cache hit? -> return
                      -> cache miss -> PostgreSQL -> fill cache -> return
```

On startup, the cache is preloaded with the most recent orders from the database.

## Configuration

All settings in `.env`. Copy `.env.example` and fill in your values.

| Variable | Default | Purpose |
|----------|---------|---------|
| `HTTP_PORT` | `8080` | listen port |
| `CACHE_CAPACITY` | `100` | max cached orders before eviction |
| `LOG_FORMAT` | `text` | logging format: text or json |
| `JWT_SECRET` | - | secret key for JWT signing (required) |
| `DB_HOST` | - | PostgreSQL host (required) |
| `DB_PORT` | - | PostgreSQL port (required) |
| `DB_USER` | - | PostgreSQL user (required) |
| `DB_PASSWORD` | - | PostgreSQL password (required) |
| `DB_NAME` | - | database name (required) |
| `DB_SSL_MODE` | `disable` | PostgreSQL SSL mode |
| `MIGRATION_DIR` | `migrations` | path to migration files |
| `KAFKA_BROKERS` | - | Kafka bootstrap servers (required) |
| `KAFKA_TOPIC` | - | topic to consume (required) |
| `RATE_LIMIT` | `5-S` | auth rate limit (login, refresh) |
| `RATE_LIMIT_REGISTER` | `3-M` | registration rate limit |

## Version

Build version and commit hash are injected at link time via `ldflags`:

```bash
make build                          # version=dev, commit=<git hash>
VERSION=1.0.0 make build            # version=1.0.0
```

The `/api/version` endpoint returns the current values. In Docker, `make up` passes `VERSION` and `COMMIT` as build args.

## Docker Compose

| Service | Role |
|---------|------|
| `db` | PostgreSQL 18 with `pg_isready` healthcheck |
| `kafka` | Apache Kafka 4.0, KRaft mode |
| `kafka-topics-setup` | creates the topic, runs once |
| `kafka-emulator` | sends test orders from `testdata/`, exits when done |
| `app` | Go binary, waits for healthy db and kafka, auto-migrates |
| `prometheus` | scrapes metrics from app on `/metrics` |
| `grafana` | dashboards, data source preconfigured to Prometheus |

All services on the `orders-network` bridge. Prometheus at `:9090`, Grafana at `:3000`. Volumes `postgres_data`, `kafka_data`, `prometheus_data`, and `grafana_data` persist across restarts. Reset with `make down`.

## Structure

```
cmd/
  main.go              entry point, wiring, graceful shutdown
  emulator/            test order producer
  gen-token/           JWT token generation utility
internal/
  app/                 dependency injection, router setup
  config/              .env loader with validation and defaults
  repository/          pgxpool, CRUD, transactions, migrations
  models/              Order, Delivery, Payment, Item, User
  dto/                 API request/response types
  cache/               custom LRU
  consumer/            Kafka consumer, deserialization, validation
  handler/             HTTP handlers (orders, auth, pages, version), cache-aside
  middleware/          JWT auth, role-based access, rate limiting, metrics
  metrics/             Prometheus counters, gauges, histograms
migrations/            golang-migrate SQL files
web/                   Go templates, static CSS
testdata/              sample order JSON files
```

Database migrations run at startup via `golang-migrate`. Orders are stored transactionally across four tables: `orders`, `delivery`, `payment`, `items`. Users stored in a separate table with bcrypt-hashed passwords. Graceful shutdown via `signal.NotifyContext` for both the HTTP server and Kafka consumer.

## Development

```
make up         # start all services
make down       # stop and remove containers and volumes
make test       # run tests with race detection
make lint       # run golangci-lint
make build      # build binary (includes swagger generation)
make swagger    # regenerate Swagger docs
make token      # generate JWT for testing
```

CI runs on every push and pull request: lint → test → docker build.

## Tests

```
make test
```

Repository tests use testcontainers (real PostgreSQL). Handler and middleware tests use mocks.