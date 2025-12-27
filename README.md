# FlashPipe

A low-latency, high-throughput event ingestion and streaming engine designed for ERP workflows. Built with Go, Redpanda (Kafka), and Redis.

## Features

- **High Throughput**: Optimized for 100k+ events/second with batching and async processing
- **Low Latency**: Sub-millisecond p99 latency with LZ4 compression
- **ERP-Ready**: Built-in support for Payroll, Attendance, and Trial Balance workflows
- **Idempotency**: Redis-backed idempotency keys prevent duplicate processing
- **Rate Limiting**: Sliding window rate limiting per tenant
- **Distributed Locking**: Redis-based locks for concurrent workflow processing
- **Circuit Breaker**: Automatic failure detection and recovery
- **Observability**: Prometheus metrics, structured JSON logging, request tracing
- **Graceful Shutdown**: Clean shutdown with in-flight request completion

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Clients   │────▶│  FlashPipe  │────▶│  Redpanda   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
                    ┌──────▼──────┐
                    │    Redis    │
                    │ (Idempotency│
                    │  Rate Limit │
                    │   Locking)  │
                    └─────────────┘
```

## Quick Start

### Prerequisites

- Go 1.21+
- Redpanda or Kafka
- Redis 7+

### Installation

```bash
# Clone the repository
git clone https://github.com/carakawedhatama/flashpipe.git
cd flashpipe

# Install dependencies
go mod tidy

# Copy environment configuration
cp .env.example .env

# Start dependencies
make docker-up

# Start prometheus
make prometheus

# Run the server
make run

# Run the consumer
make run-consumer

# Run load-test
make loadtest-light
```

### Docker Compose (Development)

```yaml
version: '3.8'
services:
  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda start
      - --smp 1
      - --memory 1G
      - --overprovisioned
      - --kafka-addr PLAINTEXT://0.0.0.0:9092
    ports:
      - "9092:9092"

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  flashpipe:
    build: .
    ports:
      - "8181:8181"
    environment:
      - KAFKA_BROKERS=redpanda:9092
      - REDIS_ADDR=redis:6379
    depends_on:
      - redpanda
      - redis
```

## API Reference

### Single Event Ingestion

```bash
POST /api/v1/events
Content-Type: application/json
X-Tenant-ID: tenant-123

{
  "idempotency_key": "unique-key-123",
  "tenant_id": "tenant-123",
  "user_id": "user-456",
  "workflow": "payroll",
  "event_type": "salary.calculated",
  "priority": "high",
  "timestamp": "2024-01-15T10:00:00Z",
  "payload": {
    "employee_id": "emp-789",
    "period_start": "2024-01-01T00:00:00Z",
    "period_end": "2024-01-31T23:59:59Z",
    "base_salary": 5000000,
    "currency": "IDR"
  }
}
```

### Batch Event Ingestion

```bash
POST /api/v1/events/batch
Content-Type: application/json
X-Tenant-ID: tenant-123

{
  "idempotency_key": "batch-unique-key",
  "tenant_id": "tenant-123",
  "workflow": "attendance",
  "priority": "normal",
  "events": [
    {
      "idempotency_key": "att-001",
      "tenant_id": "tenant-123",
      "user_id": "user-456",
      "workflow": "attendance",
      "event_type": "check_in",
      "timestamp": "2024-01-15T08:00:00Z",
      "payload": {
        "employee_id": "emp-001",
        "date": "2024-01-15T00:00:00Z",
        "status": "present"
      }
    }
  ]
}
```

### Async Event Ingestion (Fire & Forget)

```bash
POST /api/v1/events/async
Content-Type: application/json
X-Tenant-ID: tenant-123

# Same payload as single event
```

### Health Checks

```bash
# Liveness probe
GET /health/live

# Readiness probe (checks Redis & Kafka)
GET /health/ready

# Internal metrics
GET /health/metrics

# Prometheus metrics
GET /metrics
```

## ERP Workflow Types

| Workflow | Description | Typical Use Case |
|----------|-------------|------------------|
| `payroll` | Payroll processing events | Salary calculations, deductions, batch processing |
| `attendance` | Attendance tracking | Check-in/out, leave management, bulk imports |
| `trial_balance` | Financial reporting | Account entries, period closings, reconciliation |
| `general` | Generic events | Custom workflows |

## Configuration

See `.env.example` for all configuration options.

### Key Performance Tuning

| Setting | Description | Recommended |
|---------|-------------|-------------|
| `SERVER_CONCURRENCY` | Max concurrent connections | 256K for high load |
| `KAFKA_BATCH_SIZE` | Messages per batch | 1000 for throughput |
| `KAFKA_BATCH_TIMEOUT` | Max batch wait time | 10ms for latency |
| `REDIS_POOL_SIZE` | Redis connection pool | 50-100 |

## Project Structure

```
flashpipe/
├── cmd/
│   └── ingest-api/          # Main application entry point
├── internal/
│   ├── config/              # Configuration management
│   ├── handler/             # HTTP handlers
│   ├── kafka/               # Kafka producer with circuit breaker
│   ├── logging/             # Structured logging (zerolog)
│   ├── middleware/          # HTTP middleware (rate limit, metrics)
│   ├── model/               # Domain models (ERP events)
│   ├── redis/               # Redis client (idempotency, locking)
│   ├── validate/            # Request validation
│   └── worker/              # Worker pool for async processing
├── .env.example
├── go.mod
└── README.md
```

## Metrics

FlashPipe exposes Prometheus metrics at `/metrics`:

- `flashpipe_http_requests_total` - Total HTTP requests by method/path/status
- `flashpipe_http_request_duration_seconds` - Request latency histogram
- `flashpipe_http_requests_in_flight` - Current in-flight requests
- `flashpipe_events_ingested_total` - Events ingested by workflow/tenant
- `flashpipe_events_batch_size` - Batch size distribution

## License

MIT License
