# Order Service — Kafka Streams Day 1 Demo

This is the **order-service** project used for the Kafka Streams course. Each day adds smaller pieces; by the final day this becomes one full stream-processing application.

- **Day 1**: Set up Kafka Streams, simple topology (read → transform → write), topology visualization, KStream + KTable examples.
- **Day 2+**: Stateless transformations, windowing, stateful ops, joins, interactive queries, etc. (added incrementally).

## Tech

- **Spring Boot 4.x**
- **Kafka Streams** (client library, no extra cluster)
- **Java 25** (configurable in `build.gradle`)

## Prerequisites

- Java 25 (or adjust toolchain in `build.gradle`)
- Kafka at `localhost:9092`

## Topics (Day 1)

**Topics are created automatically on startup** by `KafkaTopicConfig` (using Spring Kafka’s `KafkaAdmin` and `NewTopic` beans). No manual creation needed.

- **orders** – input stream (partitions: 2)
- **orders-processed** – output stream (partitions: 2)
- **customer-reference** – KTable changelog (partitions: 1)

Override in `application.yml` with `app.kafka.topic.partitions` and `app.kafka.topic.replication-factor`. If a topic already exists, the admin skips creation (no error).

## Run

```bash
./gradlew bootRun
```

App runs on port **8080**. Override `spring.kafka.bootstrap-servers` and topic names in `application.yml` if needed.

## Day 1: What’s in the app

### Topology

- **KStream**: `orders` → transform (prefix + uppercase) → `orders-processed`
- **KTable**: `customer-reference` → in-memory table (latest value per key), for later joins/enrichment (Day 5)

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/streams/topology` | JSON: topology description, status, application ID, **mermaid** source, subtopologies/nodes |
| `GET /api/streams/topology/text` | Plain-text topology (same as `Topology#describe()`) |
| `GET /api/streams/topology/mermaid` | Mermaid flowchart source (paste at [mermaid.live](https://mermaid.live) to view) |
| `GET /api/streams/topology/visual` | **HTML page with diagram** – open in browser for a visual topology graph |
| `GET /actuator/health` | Health check |
| `GET /actuator/metrics` | All metrics (see below for stream metrics) |
| `GET /api/streams/metrics/summary` | JSON summary of kafka.* (and optionally jvm.*) metrics |
| `GET /api/streams/metrics/visual` | **Metrics dashboard** – in-app page with tables and auto-refresh |

### Monitoring stream processing metrics

**In-app dashboard:** Open **`GET /api/streams/metrics/visual`** in a browser for a simple metrics page (Kafka/streams metrics in a table, auto-refresh every 10s, optional JVM metrics).

Stream processing metrics are also exposed via **Micrometer** and the **Actuator metrics** endpoint:

1. **`KafkaStreamsMicrometerListener`** is registered on `StreamsBuilderFactoryBean` (see `StreamsTopologyConfig`). It binds the underlying `KafkaStreams` metrics to the application’s `MeterRegistry`, so they appear under `/actuator/metrics`.

2. **View all metrics:**  
   `GET /actuator/metrics`  
   Returns the list of metric names.

3. **View a specific metric:**  
   `GET /actuator/metrics/{metricName}`  
   Example: `GET /actuator/metrics/kafka.streams.consumer.commit.latency.avg`  
   Use the names from step 2 (e.g. `kafka.streams.*` for streams).

4. **Typical Kafka Streams metric names** (depend on the Kafka/client version) include:
   - Consumer: `kafka.consumer.*` (fetch rate, records lag, commit latency)
   - Producer: `kafka.producer.*` (record send rate, request latency)
   - Streams: metrics for the Streams client (e.g. commit rate, task count, state)

5. **Filter in the browser or with curl:**  
   `GET /actuator/metrics?tag=name:kafka.streams.*` or inspect the JSON and pick the metrics you need.

### Try the pipeline

Produce to `orders`:

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
# e.g.  order-1	{"customerId":"c1","amount":99.99}
```

Consume from `orders-processed`:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-processed --from-beginning
```

Produce to `customer-reference` (key\tvalue):

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic customer-reference
# e.g.  c1	{"name":"Alice","tier":"GOLD"}
```

## Project layout (Day 1)

```
src/main/java/in/codefarm/order/service/
├── OrderServiceApplication.java          # @EnableKafkaStreams
└── streams/
    ├── config/
    │   ├── StreamsTopologyConfig.java    # Binds topology, topology capture
    │   ├── TopologyDescriptionHolder.java
    │   └── TopologyCaptureCustomizer.java
    ├── topology/
    │   └── OrderStreamsTopology.java    # KStream + KTable
    └── controller/
        └── TopologyController.java      # /api/streams/topology
```

Later days will add more under `streams/` (e.g. stateless transforms, windowing, joins, interactive queries).
