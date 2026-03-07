# Day 4: Stateful Operations & Aggregations

Stateful operations keep per-key state across records: running counts, totals, or deduplication. Unlike windowed aggregations (Day 3), these are **non-windowed**: one value per key that updates as new records arrive, backed by a state store (e.g. RocksDB) and a changelog topic for recovery. This day covers state stores, **groupBy** / **groupByKey**, **count**, **reduce**, **aggregate**, retention, and exactly-once — with code and real use cases. We implement **order totals by customer and count per category** as the Day 4 use case.

---

## What You'll Learn

- **State stores**: when RocksDB is used, in-memory option, Materialized, changelog
- **groupBy** / **groupByKey**: repartitioning by key for aggregations
- **count()**: per-key count (non-windowed)
- **reduce()**: combine two values into one (same type)
- **aggregate()**: initializer + adder (different type, e.g. running total)
- State store cleanup, retention, and sizing
- Stateful filtering (e.g. deduplication)
- Exactly-once semantics with state stores
- Preview: querying state via Interactive Queries (Day 6)

---

## Stateful vs Stateless vs Windowed

| Kind | State? | Scope | Example |
|------|--------|--------|---------|
| **Stateless** (Day 2) | No | One record at a time | map, filter, branch |
| **Windowed stateful** (Day 3) | Yes, per (key, window) | Time-bounded | Hourly count per key |
| **Non-windowed stateful** (Day 4) | Yes, per key | “Forever” until cleanup | Running total per customer, count per category |

Non-windowed state grows by key: each distinct key has one stored value. Use retention/cleanup or compacted changelog to bound size.

---

## State Stores: RocksDB and In-Memory

Aggregations (count, reduce, aggregate) and KTable operations use a **state store**. By default Kafka Streams uses **RocksDB** (persistent, on disk under `state.dir`). You can choose **in-memory** for small, ephemeral state.

| Store type | When to use | Recovery |
|------------|-------------|----------|
| **RocksDB** (default) | Production; state larger than heap; need recovery from changelog | Rebuild from changelog topic on restart |
| **In-memory** | Small state, fast rebuild, or dev/test | Rebuild from changelog; lost if process dies before replay |

```java
// Default: RocksDB (no need to specify)
stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .count(Materialized.as("order-count-by-key"));

// In-memory store: use with caution (size, recovery)
stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .count(Materialized.as("order-count-by-key")
        .withStoreType(Stores.inMemoryKeyValueStore()));
```

**Changelog topic:** For each named store, Kafka Streams creates an internal changelog topic (`<application.id>-<store-name>-changelog`). Every state update is written there; on restart, the store is rebuilt by replaying the changelog. See [RocksDB and state stores](related-concepts/rocksdb-and-state-stores-deep-dive.md) for where RocksDB lives and how recovery works.

---

## Real Use Cases Where Non-Windowed State Adds Value

| Use case | Why stateful fits | Operations |
|----------|-------------------|------------|
| **Running total per customer** | One current total per customer; update on every order. | groupByKey, aggregate |
| **Count orders per category** | One count per category; increment on each order. | groupBy (new key), count |
| **Deduplication** | Drop duplicates by key (e.g. idempotency key); need to “have we seen this?” | groupByKey, reduce (keep first/last) or custom filter with store |
| **Latest value per key** | Keep only the newest event per key (e.g. latest profile). | groupByKey, reduce((a,b) -> b) |
| **Running average** | Track sum and count per key; emit average when needed. | aggregate with (sum, count) |

**Use case we implement:** **Order totals by customer and count per category** — read orders, re-key by customer for a running total (aggregate), and by category for a per-category count (groupBy + count). Implemented in the Day 4 demo (`Day4StatefulTopology`).

---

## groupByKey and groupBy

Records must be partitioned by the same key before aggregating. **groupByKey** keeps the current key; **groupBy** sets a new key (from key and/or value) and triggers a repartition.

### groupByKey — same key

Use when the stream is already keyed the way you want (e.g. customerId).

```java
KStream<String, OrderEvent> ordersByCustomer = ...;  // key = customerId
ordersByCustomer
    .groupByKey(Grouped.with(Serdes.String(), orderSerde))
    .count(Materialized.as("orders-per-customer"));
```

### groupBy — new key

Use when the aggregation key is different from the current key (e.g. current key orderId, new key categoryId).

```java
// Re-key by category for "count per category"
orders
    .groupBy((orderId, order) -> order.getCategoryId(),
             Grouped.with(Serdes.String(), orderSerde))
    .count(Materialized.as("orders-per-category"));
```

**Real use:** Same key → groupByKey. New key (e.g. category, product, region) → groupBy. Both trigger repartitioning when the key changes.

---

## count() — per-key count (non-windowed)

Increments a counter per key. No window: one count per key, updated on every record.

```java
stream
    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .count(Materialized.as("order-count-store"));

// With explicit Serdes for the store
stream
    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("order-count-store")
        .withKeySerde(Serdes.String())
        .withValueSerde(Serdes.Long()));
```

**Output:** KTable&lt;K, Long&gt;. Use `.toStream()` to get a KStream; each update emits a new record (key, newCount).

**Real use:** Orders per customer, events per device, messages per topic.

---

## reduce() — combine two values into one (same type)

Merge two values of the **same type** into one (e.g. keep latest, or concatenate). No initializer: the first value for a key is the “initial” state.

```java
// Latest value per key (replace with newer)
stream
    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .reduce((oldVal, newVal) -> newVal, Materialized.as("latest-value-store"))
    .toStream()
    .to("latest-per-key", Produced.with(Serdes.String(), Serdes.String()));

// Concatenate (e.g. append to a string log — watch size!)
stream
    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .reduce((a, b) -> a + " | " + b, Materialized.as("concat-store"));
```

**Real use:** Deduplication (keep first: `(a, b) -> a`), latest snapshot per key, or any commutative/associative merge of the same type.

---

## aggregate() — initializer + adder (any type)

Use when the **result type** is different from the **input type** (e.g. input Order, result OrderSummary with total and count). You provide an **initial value** and an **adder** that folds a new record into the aggregate.

```java
// Running total and count per customer → average when needed
stream
    .groupByKey(Grouped.with(Serdes.String(), orderSerde))
    .aggregate(
        () -> new OrderSummary(0.0, 0L),           // initializer
        (key, order, summary) -> new OrderSummary(  // adder
            summary.totalAmount() + order.getTotalAmount(),
            summary.count() + 1
        ),
        Materialized.with(Serdes.String(), orderSummarySerde)
    )
    .toStream()
    .mapValues(summary -> summary.count() > 0 ? summary.totalAmount() / summary.count() : 0.0)
    .to("customer-avg-order-value", Produced.with(Serdes.String(), Serdes.Double()));
```

**Serdes:** You must supply a Serde for the aggregate value (e.g. OrderSummary) if it’s not a built-in type.

**Real use:** Running totals, (sum, count) for averages, building a list or custom struct per key.

---

## Stateful filtering: deduplication

“Emit only the first occurrence of each key” (or “last”) is stateful: you need to remember seen keys. Use **groupByKey + reduce** and emit only when the value is “first” (e.g. a sentinel), or use the **Processor API** with a store. A simple DSL pattern:

```java
// Dedupe by key: keep first occurrence (subsequent duplicates dropped from output)
stream
    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
    .reduce((first, second) -> first, Materialized.as("dedupe-store"))
    .toStream()
    .filter((key, value) -> value != null);  // optional; reduce always has a value once seen
```

To emit only on **first** seen (not every update), use a custom **aggregate** that tracks “already emitted” or use **suppress** with a session/window; for strict “emit once per key ever,” the Processor API with a KeyValueStore is a better fit.

**Real use:** Idempotency (drop duplicate payment IDs), first-event-per-user.

---

## State store retention and cleanup

- **Non-windowed stores** do not expire by time by default. The changelog topic is usually **compacted** so old updates for the same key are discarded; the store size is bounded by the number of keys.
- **Windowed stores** (Day 3) retain windows for a **retention period**; after that, windows are dropped to free space.
- For **very high key cardinality**, consider windowing or periodic cleanup; otherwise the store and changelog can grow large.

```java
// Changelog topic config (optional): limit retention for the store’s changelog
Materialized.as("my-store")
    .withLogConfig(Map.of(
        "retention.ms", "86400000",        // 1 day
        "cleanup.policy", "compact"
    ));
```

---

## Exactly-once semantics

With Kafka Streams **exactly-once** (EOS) enabled, state store updates and output are transactional: processing a record once leads to exactly one state update and exactly one write to downstream topics. After a crash, replay does not double-count. Enable with:

```properties
processing.guarantee=exactly_once_v2
```

(Or `exactly_once` for the older variant.) Required for correctness when you cannot tolerate duplicate or missing updates.

---

## Implemented Use Case: Order Totals by Customer and Count per Category

The Day 4 demo implements two pipelines from the same **orders** stream: (1) **running order total per customer**, (2) **order count per category**.

**Pipeline (conceptually):**

1. **Read** `orders` (e.g. key = orderId, value = JSON with customerId, categoryId, amount).
2. **Branch or reuse stream:**
   - **Total by customer:** groupBy(customerId) → aggregate(initial 0.0, add amount) → write to `customer-order-totals` or expose via Interactive Query.
   - **Count by category:** groupBy(categoryId) → count() → write to `orders-per-category` or expose via Interactive Query.
3. **Materialized** stores (e.g. `customer-totals-store`, `orders-per-category-store`) back both; changelog topics enable recovery.

**Code (conceptual):** See `Day4StatefulTopology` in the Day4-Demo.

```java
KStream<String, String> orders = builder.stream("orders", Consumed.with(Serdes.String(), Serdes.String()));

// Parse and filter (reuse Day 2 style)
KStream<String, OrderEvent> valid = orders
    .filter((k, v) -> v != null && !v.isBlank())
    .mapValues(this::parseOrderOrNull)
    .filter((k, v) -> v != null);

// Running total per customer
valid
    .groupBy((orderId, order) -> order.getCustomerId(), Grouped.with(Serdes.String(), orderSerde))
    .aggregate(
        () -> 0.0,
        (key, order, total) -> total + order.getTotalAmount(),
        Materialized.as("customer-totals-store")
    )
    .toStream()
    .to("customer-order-totals", Produced.with(Serdes.String(), Serdes.Double()));

// Count per category
valid
    .groupBy((orderId, order) -> order.getCategoryId(), Grouped.with(Serdes.String(), orderSerde))
    .count(Materialized.as("orders-per-category-store"))
    .toStream()
    .to("orders-per-category", Produced.with(Serdes.String(), Serdes.Long()));
```

**How to run:** Start the Day4-Demo app (`kafka-streams/Day4-Demo/order-service`): `./gradlew bootRun`. POST orders via `POST /api/orders` with JSON body `{"customerId":"c1","totalAmount":100.0,"categoryId":"electronics"}`. Consume `customer-order-totals` and `orders-per-category`, or query the stores via Interactive Queries (Day 6 pattern).

---

## Best Practices

- Prefer **RocksDB** for production; use **in-memory** only for small, easily rebuilt state.
- Use **groupByKey** when the key is already correct; **groupBy** when you need a different key (e.g. category, region).
- Use **count()** for simple counts; **reduce()** when merging same-type values; **aggregate()** when the result type differs (totals, averages, custom structs).
- Set **state.dir** to a stable path (not `/tmp`) so state survives restarts; rely on the changelog for recovery.
- Enable **exactly_once_v2** when you need exactly-once semantics.
- Monitor **state store size** and changelog retention; use retention/cleanup for high cardinality.

---

## Summary

| Operation | Purpose | Output type |
|-----------|---------|-------------|
| **groupByKey** | Partition by existing key | — |
| **groupBy** | Re-key then partition | — |
| **count()** | Per-key count | KTable&lt;K, Long&gt; |
| **reduce()** | Merge two same-type values | KTable&lt;K, V&gt; |
| **aggregate()** | Initial + fold to any type | KTable&lt;K, VR&gt; |
| **Materialized.as(name)** | Named store (RocksDB default) | — |
| **Changelog** | Backup for store; recovery | &lt;app-id&gt;-&lt;store-name&gt;-changelog |

**Next:** Day 5 — joins (stream-stream, stream-table, table-table) and enrichment patterns.

---

## Resources

- [Kafka Streams – Aggregating](https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#aggregating)
- [Materialized (stores)](https://kafka.apache.org/34/javadoc/org/apache/kafka/streams/kstream/Materialized.html)
- [State stores and RocksDB](related-concepts/rocksdb-and-state-stores-deep-dive.md)
- [Confluent: Stateful processing](https://developer.confluent.io/courses/kafka-streams/stateful-operations/)
