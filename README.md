# gorder

Event-driven order processing service. Kafka ingestion, PostgreSQL persistence, in-memory LRU cache, REST API. Built with Go.

## Quick start

```
cp .env.example .env
docker compose up --build
```

The service starts at `http://localhost:8080`. The emulator sends test orders from `testdata/` to Kafka on first run.

## API

```
GET /                    all orders, HTML
GET /:uid                single order, HTML
GET /api/orders/:uid     order by UID, JSON
GET /api/all_orders_uids all UIDs, JSON
GET /api/server_check    server liveness
GET /api/db_check        database connectivity
GET /api/kafka_check     Kafka connectivity
```

Cache-aside: `GetOrderByUIDHandle` and `OrderPageHandle` check the cache first, fall back to PostgreSQL on miss, then populate the cache.

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
| `DB_HOST` | `db` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | - | PostgreSQL user |
| `DB_PASSWORD` | - | PostgreSQL password |
| `DB_NAME` | - | database name |
| `MIGRATION_DIR` | `migrations` | path to migration files |
| `KAFKA_BROKERS` | `kafka:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `wbl0_orders` | topic to consume |

## Docker Compose

| Service | Role |
|---------|------|
| `db` | PostgreSQL with `pg_isready` healthcheck |
| `kafka` | Bitnami Kafka, KRaft mode |
| `kafka-topics-setup` | creates the topic, runs once |
| `app` | Go binary, waits for healthy db and kafka |
| `kafka-emulator` | sends test orders, exits when done |

All services on the `orders-network` bridge. Volumes `postgres_data` and `kafka_data` persist across restarts. Reset with `docker compose down -v`.

## Structure

```
cmd/
  main.go              entry point, wiring, graceful shutdown
  emulator/            test order producer
internal/
  config/              .env loader
  database/            pgxpool, CRUD, migrations
  models/              Order, Delivery, Payment, Item, validation
  cache/               LRU (doubly-linked list, map, RWMutex)
  kafka/               consumer, deserialization, validation
  handlers/            HTTP handlers, cache-aside, Gin
migrations/            golang-migrate SQL files
web/                   Go templates, static CSS
testdata/              sample order JSON files
```

Database migrations run at startup via `golang-migrate`. Orders are stored transactionally across four tables: `orders`, `delivery`, `payment`, `items`. Graceful shutdown via `signal.NotifyContext` for both the HTTP server and Kafka consumer.

## Tests

```
go test ./internal/...
```

Covers `cache/`, `models/`, and `handlers/`. Handler tests use a mock `StorageInterface`. Cache tests cover set, get, eviction, delete, and `GetAllUIDs`.
