# Order Service — Kafka Streams Day 2 Demo

This is the **order-service** in Day2-Demo: **stateless transformations** (mapValues, filter, selectKey, split/branch), REST API to place orders for different scenarios, and handling malformed data.

## Tech

- **Spring Boot 4.x**
- **Kafka Streams** (stateless DSL)
- **Java 25** (configurable in `build.gradle`)

## Prerequisites

- Java 25 (or adjust toolchain in `build.gradle`)
- Kafka at `localhost:9092`

## Topics (Day 2)

**Topics are created automatically on startup** by `KafkaTopicConfig`.

- **orders** – input (orders placed via REST or producer)
- **high-value-orders** – branch: orders with `totalAmount >= 1000`
- **normal-orders** – branch: all other valid orders

Override in `application.yml` with `app.kafka.*` and `app.kafka.topic.*`.

## Run

```bash
./gradlew bootRun
```

App runs on port **8080**.

### Dashboard (Thymeleaf)

Open **http://localhost:8080/**.

- **Simulate (client):** Send one order or **bulk** (count 1–500, scenario). Events go to input topic only; client has no access to processing result.
- **Dashboard:** Data from **processed results only** — consumed from `high-value-orders` and `normal-orders`. Counts and recent records reflect what the stream produced.
- **Alerts:** Raised only when **processed** data triggers them (e.g. order routed to high-value, or count threshold reached). Config: `app.dashboard.alert.high-value-count-threshold`, `normal-count-threshold`.

---

## Day 2: What’s in the app

### Topology (stateless)

1. **Read** from `orders` (String key, JSON value).
2. **Filter** – drop null/empty.
3. **mapValues** – parse JSON to `OrderEvent`; return null if malformed (dropped).
4. **Filter** – keep only valid orders (`orderId`, `customerId`, `totalAmount` present and valid).
5. **selectKey** – re-key by `customerId` (for future joins).
6. **peek** – log (side effect).
7. **split/branch** – first branch: high-value (`totalAmount >= 1000`); default branch: normal.
8. **to** – `high-value-orders` and `normal-orders`.

Malformed JSON or invalid records are dropped in the parse/filter steps.

### REST: Place order (different scenarios)

**POST** `/api/orders`  
Body (JSON): `orderId`, `customerId`, `totalAmount`, `status`, optional `scenario`.  
Query param: `?scenario=...` overrides body.

| Scenario   | Query or body `scenario` | Effect |
|-----------|---------------------------|--------|
| **valid** | `valid` (default)         | Sends a valid order (orderId, customerId, totalAmount, status). |
| **high-value** | `high-value`        | Sends an order with `totalAmount >= 1000` → stream routes to `high-value-orders`. |
| **normal** | `normal`                 | Sends an order with `totalAmount < 1000` → stream routes to `normal-orders`. |
| **malformed** | `malformed`           | Sends invalid JSON to the topic → stream drops it (parse fails). |

**Example**

```bash
# Valid order (default)
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"o1","customerId":"c1","totalAmount":150.0,"status":"PLACED"}'

# High-value order (branch demo)
curl -X POST "http://localhost:8080/api/orders?scenario=high-value" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"c2","totalAmount":2500.0}'

# Normal order
curl -X POST "http://localhost:8080/api/orders?scenario=normal" \
  -H "Content-Type: application/json" \
  -d '{"customerId":"c3","totalAmount":99.99}'

# Malformed (stream will drop)
curl -X POST "http://localhost:8080/api/orders?scenario=malformed" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Streams & topology endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/streams/topology` | JSON: topology, mermaid, subtopologies |
| `GET /api/streams/topology/visual` | HTML topology diagram |
| `GET /api/streams/topology/mermaid` | Mermaid source |
| `GET /api/streams/metrics/visual` | Metrics dashboard |
| `GET /actuator/health` | Health |
| `GET /actuator/metrics` | All metrics |

### Consume branch outputs

```bash
# High-value orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic high-value-orders --from-beginning

# Normal orders
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic normal-orders --from-beginning
```

---

## Project layout (Day 2)

```
src/main/java/in/codefarm/order/service/
├── OrderServiceApplication.java
├── api/
│   ├── OrderController.java           # POST /api/orders (place order, scenarios)
│   └── dto/
│       ├── OrderRequest.java          # REST body
│       └── OrderEvent.java            # Parsed in streams (orderId, customerId, totalAmount, status)
└── streams/
    ├── config/
    │   ├── StreamsTopologyConfig.java
    │   ├── KafkaTopicConfig.java      # orders, high-value-orders, normal-orders
    │   ├── TopologyDescriptionHolder.java
    │   └── TopologyCaptureCustomizer.java
    ├── topology/
    │   └── Day2StatelessTopology.java # mapValues, filter, selectKey, split/branch
    └── controller/
        ├── TopologyController.java
        └── StreamsMetricsController.java
```

---

## Next

Day 3 will add windowing and time concepts (tumbling, hopping, session windows, event time, grace period).
