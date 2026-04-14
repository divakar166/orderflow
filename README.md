# OrderFlow

> **An event-driven order processing pipeline built with FastAPI, Apache Kafka, and PostgreSQL.**

OrderFlow simulates a real-world e-commerce backend where every order placement triggers an asynchronous multi-stage processing pipeline — validation → inventory → payment → finalization — all orchestrated through Kafka topics. It demonstrates key distributed systems patterns: event-driven architecture, worker-based consumers, retry logic, and full audit event logging.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Order Lifecycle](#order-lifecycle)
- [Kafka Topics](#kafka-topics)
- [Order Statuses](#order-statuses)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Running the Producer](#running-the-producer)
- [API Reference](#api-reference)
- [Development Notes](#development-notes)

---

## Architecture Overview

```
                      ┌─────────────────────────────────────────────────┐
                      │                  FastAPI Server                 │
                      │  POST /orders  ──►  OrderService  ──►  DB Save  │
                      │                         │                       │
                      │                   Kafka Publish                 │
                      │                  (orders.created)               │
                      └─────────────────────────┬───────────────────────┘
                                                │
                    ┌───────────────────────────▼───────────────────────┐
                    │                    Kafka Broker                   │
                    │                                                   │
                    │   orders.created  ──►  orders.processing          │
                    │   orders.processing ──►  orders.completed         │
                    │                      └──►  orders.failed          │
                    └───────┬──────────────────┬──────────────┬─────────┘
                            │                  │              │
                      ┌─────▼─────┐  ┌─────────▼───┐  ┌───────▼───────┐
                      │Validation │  │  Inventory  │  │    Payment    │
                      │  Worker   │  │   Worker    │  │    Worker     │
                      └─────┬─────┘  └──────┬──────┘  └───────┬───────┘
                            │               │                 │
                    ┌───────▼───────────────▼─────────────────▼───────┐
                    │                 Finalizer Worker                │
                    │      (orders.completed + orders.failed)         │
                    │      Writes terminal state to PostgreSQL        │
                    └─────────────────────────────────────────────────┘
```

Each worker runs as an independent `asyncio.Task` inside the same FastAPI process, consuming from its designated Kafka topic and updating order state in PostgreSQL.

---

## Tech Stack

| Layer               | Technology                                                                                                |
| ------------------- | --------------------------------------------------------------------------------------------------------- |
| **API Framework**   | [FastAPI](https://fastapi.tiangolo.com/) + [Uvicorn](https://www.uvicorn.org/)                            |
| **Message Broker**  | [Apache Kafka](https://kafka.apache.org/) via [aiokafka](https://aiokafka.readthedocs.io/)                |
| **Database**        | [PostgreSQL 16](https://www.postgresql.org/)                                                              |
| **ORM**             | [SQLAlchemy 2.0](https://docs.sqlalchemy.org/) (async) + [asyncpg](https://magicstack.github.io/asyncpg/) |
| **Migrations**      | [Alembic](https://alembic.sqlalchemy.org/)                                                                |
| **Config**          | [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)                         |
| **Data Validation** | [Pydantic v2](https://docs.pydantic.dev/)                                                                 |
| **Load Producer**   | [Faker](https://faker.readthedocs.io/) + [httpx](https://www.python-httpx.org/)                           |
| **Infrastructure**  | [Docker Compose](https://docs.docker.com/compose/)                                                        |
| **Python**          | 3.12+                                                                                                     |

---

## Project Structure

```
orderflow/
├── main.py                     # FastAPI app entry point; starts workers via lifespan
├── producer.py                 # Standalone CLI script to generate fake orders
├── docker-compose.yml          # Zookeeper, Kafka, PostgreSQL services
├── pyproject.toml              # Project metadata and dependencies (uv)
├── requirements.txt            # pip-compatible dependency list
├── .env                        # Environment variables (not committed)
│
├── api/
│   └── orders.py               # REST endpoints: POST, GET /orders
│
├── core/
│   ├── config.py               # Pydantic Settings — loads from .env
│   ├── database.py             # Async SQLAlchemy engine, session factory
│   └── kafka.py                # Singleton Kafka producer with idempotence
│
├── models/
│   ├── order.py                # SQLAlchemy Order model + OrderStatus/FailureType enums
│   └── event.py                # Audit event log model
│
├── schemas/
│   └── order.py                # Pydantic request/response schemas
│
├── repo/
│   ├── order_repo.py           # Data access layer for orders
│   └── event_repo.py           # Data access layer for audit events
│
├── services/
│   ├── order_service.py        # Business logic: create, get, list orders
│   └── event_service.py        # Audit event logging
│
└── workers/
    ├── base_worker.py          # Abstract base: Kafka consumer loop + error handling
    ├── validation_worker.py    # Validates order data; publishes to processing
    ├── inventory_worker.py     # Checks stock availability; publishes to processing/failed
    ├── payment_worker.py       # Processes payment with retry logic
    └── finalizer_worker.py     # Writes terminal status (COMPLETED / FAILED) to DB
```

---

## Order Lifecycle

Every order flows through the following sequential stages. Each stage is handled by a dedicated async worker:

```
[POST /orders/]
      │
      ▼
  NEW_ORDER  ──► Kafka: orders.created
      │
      ▼
  VALIDATING  (ValidationWorker)
      ├─ PASS (90%) ──► VALIDATED  ──► Kafka: orders.processing [stage=INVENTORY]
      └─ FAIL (10%) ──► VALIDATION_FAILED  ──► Kafka: orders.failed
      │
      ▼
  INVENTORY_CHECKING  (InventoryWorker)
      ├─ PASS (85%) ──► INVENTORY_RESERVED  ──► Kafka: orders.processing [stage=PAYMENT]
      └─ FAIL (15%) ──► OUT_OF_STOCK  ──► Kafka: orders.failed
      │
      ▼
  PAYMENT_PENDING  (PaymentWorker)
      ├─ PASS (80%) ──► PAYMENT_SUCCESS  ──► Kafka: orders.completed
      ├─ FAIL + retries < 3 ──► RETRY_PENDING  ──► Kafka: orders.processing [stage=PAYMENT]
      └─ FAIL + retries exhausted ──► PAYMENT_FAILED  ──► Kafka: orders.failed
      │
      ▼
  FinalizerWorker (consumes orders.completed + orders.failed)
      ├─ COMPLETED  ✓
      └─ FAILED     ✗
```

> **Pass rates are configurable** via `.env`. See [Configuration](#configuration).

---

## Kafka Topics

| Topic               | Producer                                               | Consumer                           | Purpose                                                                         |
| ------------------- | ------------------------------------------------------ | ---------------------------------- | ------------------------------------------------------------------------------- |
| `orders.created`    | `OrderService`                                         | `ValidationWorker`                 | New orders awaiting validation                                                  |
| `orders.processing` | `ValidationWorker`, `InventoryWorker`, `PaymentWorker` | `InventoryWorker`, `PaymentWorker` | Orders progressing through pipeline stages (routed by `stage` field in payload) |
| `orders.completed`  | `PaymentWorker`                                        | `FinalizerWorker`                  | Successfully paid orders                                                        |
| `orders.failed`     | `ValidationWorker`, `InventoryWorker`, `PaymentWorker` | `FinalizerWorker`                  | Orders that failed at any stage                                                 |

The `orders.processing` topic uses a **stage-routing pattern**: the payload carries a `"stage"` field (`"INVENTORY"` or `"PAYMENT"`), and each worker only processes messages intended for its stage, ignoring others.

---

## Order Statuses

| Status               | Description                              |
| -------------------- | ---------------------------------------- |
| `NEW_ORDER`          | Order created, published to Kafka        |
| `VALIDATING`         | Validation in progress                   |
| `VALIDATED`          | Validation passed                        |
| `VALIDATION_FAILED`  | Validation failed (terminal)             |
| `INVENTORY_CHECKING` | Inventory check in progress              |
| `INVENTORY_RESERVED` | Stock allocated                          |
| `OUT_OF_STOCK`       | Product unavailable (terminal)           |
| `PAYMENT_PENDING`    | Payment attempt in progress              |
| `RETRY_PENDING`      | Payment failed, scheduled for retry      |
| `PAYMENT_SUCCESS`    | Payment succeeded                        |
| `PAYMENT_FAILED`     | Payment exhausted all retries (terminal) |
| `COMPLETED`          | Order fully processed and confirmed ✓    |
| `FAILED`             | Generic terminal failure state ✗         |

---

## Configuration

All configuration is managed via environment variables, loaded through `core/config.py` using **Pydantic Settings**. Create a `.env` file in the project root:

```env
# Database
DATABASE_URL=postgresql+asyncpg://order_user:order_pass@localhost:5432/order_db

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Kafka Topic Names
KAFKA_TOPIC_CREATED=orders.created
KAFKA_TOPIC_PROCESSING=orders.processing
KAFKA_TOPIC_FAILED=orders.failed
KAFKA_TOPIC_COMPLETED=orders.completed

# Pass rate simulation (0.0 to 1.0)
VALIDATION_PASS_RATE=0.90
INVENTORY_PASS_RATE=0.85
PAYMENT_PASS_RATE=0.80

# Payment retry config
MAX_RETRY_COUNT=3
```

| Variable                  | Default      | Description                                   |
| ------------------------- | ------------ | --------------------------------------------- |
| `DATABASE_URL`            | _(required)_ | Async PostgreSQL connection string            |
| `KAFKA_BOOTSTRAP_SERVERS` | _(required)_ | Kafka broker address                          |
| `VALIDATION_PASS_RATE`    | `0.90`       | Probability an order passes validation        |
| `INVENTORY_PASS_RATE`     | `0.85`       | Probability inventory is available            |
| `PAYMENT_PASS_RATE`       | `0.80`       | Probability payment succeeds per attempt      |
| `MAX_RETRY_COUNT`         | `3`          | Maximum payment retry attempts before failing |

---

## Getting Started

### Prerequisites

- Python 3.12+
- Docker & Docker Compose
- [`uv`](https://github.com/astral-sh/uv) (recommended) or `pip`

### 1. Clone the repository

```bash
git clone https://github.com/divakar166/orderflow.git
cd orderflow
```

### 2. Start infrastructure

Spin up Kafka (+ Zookeeper) and PostgreSQL using Docker Compose:

```bash
docker compose up -d
```

This starts:

- **Zookeeper** on port `2181`
- **Kafka** on port `9092`
- **PostgreSQL** on port `5432` with database `order_db`

### 3. Install dependencies

**Using `uv` (recommended):**

```bash
uv sync
```

**Using `pip`:**

```bash
pip install -r requirements.txt
```

### 4. Configure environment

Copy and adjust the `.env` file (defaults match the Docker Compose config):

```bash
cp .env.example .env   # or create it manually — see Configuration section
```

### 5. Start the API server

```bash
uvicorn main:app --reload
```

The server will:

- Create all database tables automatically on startup
- Start all four Kafka consumer workers as background tasks
- Expose the API at `http://localhost:8000`
- Expose interactive docs at `http://localhost:8000/docs`

---

## Running the Producer

`producer.py` is a standalone CLI script that uses **Faker** to generate realistic fake orders and POST them to the running API. Use this to load-test the pipeline.

```bash
# Send 20 orders with 1 second delay (default)
python producer.py

# Send 50 orders
python producer.py --count 50

# Send 100 orders with 0.3s delay between each
python producer.py --count 100 --delay 0.3
```

**Example output:**

```
2026-04-14 09:00:01 | INFO | Producer starting — sending 50 orders with 0.5s delay each
2026-04-14 09:00:01 | INFO | Target API: http://localhost:8000
----------------------------------------------------------------------
2026-04-14 09:00:01 | INFO | [   1] ✓ Created order a3f9b12c... | Mechanical Keyboard x2 @ ₹149.99 = ₹299.98
2026-04-14 09:00:01 | INFO | [   2] ✓ Created order d7e4c89a... | Running Shoes Pro x1 @ ₹89.50 = ₹89.50
...
----------------------------------------------------------------------
2026-04-14 09:00:26 | INFO | Done. Sent: 50 | Success: 50 | Failed: 0
```

The producer runs **asynchronously** using `httpx.AsyncClient`, making it efficient for high-volume testing.

---

## API Reference

All endpoints are documented interactively at `http://localhost:8000/docs`.

### `POST /orders/`

Create a new order. Saves to the database and publishes to `orders.created` Kafka topic.

**Request Body:**

```json
{
  "customer_id": "uuid-string",
  "product": "Mechanical Keyboard",
  "category": "Electronics",
  "quantity": 2,
  "price": 149.99
}
```

**Response `201 Created`:**

```json
{
  "order_id": "a3f9b12c-...",
  "customer_id": "uuid-string",
  "product": "Mechanical Keyboard",
  "category": "Electronics",
  "quantity": 2,
  "price": 149.99,
  "total": 299.98,
  "order_status": "NEW_ORDER",
  "created_at": "2026-04-14T09:00:01"
}
```

---

### `GET /orders/{order_id}`

Retrieve the current state of a specific order, including its status, stage, and any failure information.

**Response `200 OK`:**

```json
{
  "order_id": "a3f9b12c-...",
  "order_status": "COMPLETED",
  "current_stage": "DONE",
  "validation_status": "PASSED",
  "inventory_status": "RESERVED",
  "payment_status": "SUCCESS",
  "retry_count": 0,
  "completed_at": "2026-04-14T09:00:03"
}
```

---

### `GET /orders/`

List all orders with pagination.

| Query Param | Default | Max   | Description                |
| ----------- | ------- | ----- | -------------------------- |
| `limit`     | `50`    | `200` | Number of orders to return |
| `offset`    | `0`     | —     | Number of orders to skip   |

---

### `GET /health`

Returns `{"status": "ok"}`. Use for liveness checks.

---

## Development Notes

### Worker Design

All workers inherit from `BaseWorker`, which encapsulates:

- Kafka consumer lifecycle (`start` / `stop`)
- Message deserialization (JSON)
- Error isolation per message (exceptions don't kill the consumer loop)
- Graceful cancellation on shutdown

Subclasses only need to declare `consumer_group`, `topics`, and implement `handle_message(payload: dict)`.

### Idempotent Kafka Producer

The Kafka producer (`core/kafka.py`) is configured with:

- `acks="all"` — waits for all in-sync replica acknowledgements
- `enable_idempotence=True` — prevents duplicate messages on retries
- Lazy singleton initialization — started on first use, stopped during app shutdown

### Audit Event Log

Every state transition is recorded in an `order_events` table via `EventService`. This gives a full audit trail for each order, useful for debugging and observability.

### Graceful Shutdown

On SIGTERM/SIGINT, the FastAPI lifespan handler:

1. Cancels all worker `asyncio.Task`s
2. Awaits their clean shutdown (via `CancelledError` propagation)
3. Stops the Kafka producer
4. Logs any unexpected errors from workers

### Stopping Docker services

```bash
docker compose down          # Stop containers
docker compose down -v       # Stop containers and delete volumes (wipes DB data)
```
