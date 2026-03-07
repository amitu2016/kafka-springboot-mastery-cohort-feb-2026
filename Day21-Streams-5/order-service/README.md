-# Day 5 Demo: Stream-Table Join — Order Enrichment

**Orders** (KStream) + **customers** (KTable) → **enriched-orders** (KStream). Each order is re-keyed by `customerId` and joined with the customers table; output is order + customer name/tier.

## Prerequisites

- Java 25 (or adjust in `build.gradle`)
- Kafka at `localhost:9092`

## Topics

- **orders** — input stream (key = orderId or any, value = JSON with `orderId`, `customerId`, `totalAmount`, `categoryId`)
- **customers** — table (key = customerId, value = JSON with `customerId`, `name`, `tier`)
- **enriched-orders** — output (key = customerId, value = JSON with order fields + `customerName`, `customerTier`)

## Run

```bash
./gradlew bootRun
```

App runs on port **8083**.

### Dashboard (Thymeleaf)

Open **http://localhost:8083/**.

- **Add customer:** Post to the customers table (customerId, name, tier). Do this first so orders can be enriched.
- **Simulate orders:** Send one order or bulk (count 1–500, customer prefix). Orders go to input topic only.
- **Dashboard:** Data from **processed results only** — consumed from `enriched-orders`. Each row shows order + customer name/tier from the stream-table join.

## Endpoints

| Endpoint | Description |
|----------|-------------|
| `POST /api/orders` | Send an order event. Body: JSON with `customerId`, `totalAmount`, optional `orderId`, `categoryId`. |
| `GET /api/streams/topology` | Topology JSON + sub-topologies |
| `GET /api/streams/topology/visual` | Mermaid diagram of topology |
| `GET /actuator/health` | Health |

## Try it

```bash
# Add a customer (key = customerId)
curl -X POST http://localhost:8083/customers -d "customerId=customer-1&name=Alice&tier=gold"

# Send an order for that customer
curl -X POST http://localhost:8083/orders -d "customerId=customer-1&totalAmount=99.99&categoryId=electronics"

# Or via API with JSON
curl -X POST http://localhost:8083/api/orders -H "Content-Type: application/json" \
  -d '{"orderId":"o1","customerId":"customer-1","totalAmount":100.0,"categoryId":"electronics"}'

# Consume enriched orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic enriched-orders --from-beginning --property print.key=true
```

## Config

`application.yml`:

- `app.kafka.input-topic` — orders topic (default `orders`)
- `app.kafka.customers-topic` — customers table topic (default `customers`)
- `app.kafka.enriched-orders-topic` — output (default `enriched-orders`)
- `state.dir` — Kafka Streams state directory (optional)

## Next

Day 6 — Interactive Queries, state store discovery, deployment.
