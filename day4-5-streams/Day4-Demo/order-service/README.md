# Day 4 Demo: Stateful Operations — Order Totals & Count per Category

Non-windowed stateful aggregations: **running order total per customer** (aggregate) and **order count per category** (count). Reads from `orders`, writes to `customer-order-totals` and `orders-per-category`. State is stored in RocksDB (see `state.dir`).

## Prerequisites

- Java 25 (or adjust in `build.gradle`)
- Kafka at `localhost:9092`

## Topics

- **orders** — input (key = any, value = JSON with `customerId`, `totalAmount`, optional `categoryId`)
- **customer-order-totals** — output (key = customerId, value = Double = running total)
- **orders-per-category** — output (key = categoryId, value = Long = count)

## Run

```bash
./gradlew bootRun
```

App runs on port **8082**.

### Dashboard (Thymeleaf)

Open **http://localhost:8082/**.

- **Simulate (client):** Send one order (customerId, totalAmount, categoryId) or **bulk** (count 1–500, customer prefix). Orders go to input topic only; client has no access to processing result.
- **Dashboard:** Data from **processed results only** — consumed from `customer-order-totals` (running total per customer) and `orders-per-category` (count per category).
- **Alerts:** Raised when a customer total or category count from the stream crosses the configured threshold. Config: `app.dashboard.alert.total-threshold`, `app.dashboard.alert.category-count-threshold`.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/orders` | Send an order event. Body: JSON with `customerId`, `totalAmount`, optional `categoryId`. Query `?key=...` to set Kafka key. |
| `GET /api/streams/topology` | Topology JSON + sub-topologies |
| `GET /api/streams/topology/visual` | Mermaid diagram of topology |
| `GET /actuator/health` | Health |

## Try it

```bash
# Send orders (different customers and categories)
curl -X POST http://localhost:8082/api/orders -H "Content-Type: application/json" \
  -d '{"customerId":"c1","totalAmount":100.0,"categoryId":"electronics"}'
curl -X POST http://localhost:8082/api/orders -H "Content-Type: application/json" \
  -d '{"customerId":"c1","totalAmount":50.0,"categoryId":"books"}'
curl -X POST http://localhost:8082/api/orders -H "Content-Type: application/json" \
  -d '{"customerId":"c2","totalAmount":200.0,"categoryId":"electronics"}'

# Consume running totals per customer
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic customer-order-totals --from-beginning --property print.key=true

# Consume count per category
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders-per-category --from-beginning --property print.key=true
```

## Config

`application.yml`:

- `app.kafka.input-topic` — input topic (default `orders`)
- `app.kafka.customer-totals-topic` — output for running totals (default `customer-order-totals`)
- `app.kafka.orders-per-category-topic` — output for count per category (default `orders-per-category`)
- **`state.dir`** — set to `${user.dir}/kafka-streams-state`; state store under project root. Inspect: `./kafka-streams-state/order-service-streams-day4/<task-id>/rocksdb/`

## Next

Day 5 — joins (stream-stream, stream-table, table-table) and enrichment.
