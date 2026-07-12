# gorder

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go)](https://go.dev/)
[![CI](https://github.com/venexene/gorder/actions/workflows/ci.yml/badge.svg)](https://github.com/venexene/gorder/actions)
[![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

Event-driven order processing service. Kafka ingestion, PostgreSQL persistence, in-memory LRU cache, REST API, JWT auth, Prometheus and Grafana metrics, structured logging. Built with Go.

## Quick start

```
cp .env.example .env
make up
```

The service starts at `http://localhost:8080`. The emulator sends test orders from `testdata/` to Kafka automatically on first run.

## API

Public endpoints (no auth):

```
GET  /health/live         server liveness
GET  /health/ready        database and Kafka connectivity
GET  /login               login page (HTML)
POST /login               authenticate, sets httpOnly cookies
GET  /register            registration page (HTML)
POST /register            create new user
POST /refresh             get new access token from refresh token
POST /logout              clear cookies
```

Protected endpoints (JWT required, cookie or Authorization header):

```
GET  /                    all orders, HTML (user and admin)
GET  /:uid                single order, HTML (user and admin)
GET  /api/orders/:uid     order by UID, JSON (admin only)
GET  /api/all_orders_uids all UIDs, JSON (admin only)
```

## Authentication

- Passwords hashed with bcrypt (cost 10)
- Tokens stored in httpOnly cookies for browser or Authorization header for API
- Access token: 15 minutes, refresh token: 7 days with rotation
- Browsers are redirected to `/login` when unauthenticated
- Role-based access: `admin` for all endpoints, `user` for HTML pages only
- Default admin created by migration: username - `admin`, password - `admin`

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
  handler/             HTTP handlers (orders, auth, pages), cache-aside
  middleware/          JWT auth, role-based access, metrics
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
make build      # build binary
make token      # generate JWT for testing
```

CI runs on every push and pull request: lint → test → docker build.

## Tests

```
make test
```

Repository tests use testcontainers (real PostgreSQL). Handler and middleware tests use mocks.
Covers orders, auth (login/register/refresh/logout), cache, config, consumer.
