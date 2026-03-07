# Day 3 Demo: Windowing — Hourly Order Counts

Tumbling 1-hour windows with grace period for late events. Reads from `orders`, counts per key per window, writes to `order-counts-hourly`.

## Prerequisites

- Java 25 (or adjust in `build.gradle`)
- Kafka at `localhost:9092`

## Topics

- **orders** — input (key = e.g. customerId, value = any string/JSON)
- **order-counts-hourly** — output (windowed key, Long count)

## Run

```bash
./gradlew bootRun
```

App runs on port **8080**.

### Dashboard (Thymeleaf)

Open **http://localhost:8080/**.

- **Simulate (client):** Send one event or **bulk** (count 1–500, key prefix). Events go to input topic only; client has no access to processing result.
- **Dashboard:** Data from **processed results only** — consumed from `order-counts-hourly`. Windowed counts reflect what the stream produced.
- **Alerts:** Raised only when a **processed** windowed count crosses the threshold. Config: `app.dashboard.alert.count-threshold` (default 5).

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/orders` | Send an order event. Body: JSON (optional). Query `?key=customer-1` to set Kafka key. |
| `GET /api/streams/topology` | Topology JSON + sub-topologies |
| `GET /api/streams/topology/visual` | Mermaid diagram of topology |
| **`GET /api/store/hourly-counts`** | **Interactive Queries** — windowed counts from the state store (RocksDB). Optional: `?key=...` (filter by key), `?from=&to=` (epoch seconds). Returns 503 if Streams not RUNNING. |
| `GET /actuator/health` | Health |

The dashboard shows two sources: (1) **from topic** — consumer of `order-counts-hourly`; (2) **from state store** — same data via Interactive Queries (`KafkaStreams#store()`).

## Try it

```bash
# Send a few orders (key = customer-1)
curl -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{"customerId":"customer-1","orderId":"o1"}'
curl -X POST http://localhost:8080/api/orders -H "Content-Type: application/json" -d '{"customerId":"customer-1","orderId":"o2"}'
curl -X POST "http://localhost:8080/api/orders?key=customer-2" -H "Content-Type: application/json" -d '{"orderId":"o3"}'

# Consume windowed counts (key is Windowed<String>, value is Long)
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic order-counts-hourly --from-beginning --property print.key=true
```

## Config

`application.yml`:

- `app.windowing.size-hours` — window size (default 1)
- `app.windowing.grace-minutes` — grace period for late events (default 5)
- **`state.dir`** — set to `${user.dir}/kafka-streams-state` so the RocksDB state store is created under the project root. After running and sending events, inspect: `./kafka-streams-state/order-service-streams-day3/<task-id>/rocksdb/hourly-order-counts/`

## Next

Day 4 — stateful operations and aggregations (non-windowed count, reduce, aggregate).
