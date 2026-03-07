# Day 5: Advanced Joins & Enrichment Patterns

Joins combine two streams or tables by key so you can correlate events (order + payment) or enrich a stream with lookup data (order + customer profile). Kafka Streams supports **stream-stream**, **stream-table**, and **table-table** joins, with **join windows** for time-bounded stream-stream correlation and **enrichment** via stream-table lookups. This day covers join types, JoinWindows, null handling, and real use cases. We implement **order enrichment with customer data (stream-table)** as the Day 5 use case.

---

## What You'll Learn

- **Stream-stream joins**: inner, left, outer; time-bounded with JoinWindows
- **Stream-table joins**: enrich each stream record with the latest table value for the key
- **Table-table joins**: combine two KTables (both keyed the same)
- **JoinWindows**: time difference and grace period for stream-stream
- **Foreign-key table-table joins**: join when keys differ (e.g. order.customerId → customer)
- **Enrichment patterns**: lookup, dimension tables, handling nulls
- **Join semantics**: when results are emitted; null keys and null values

---

## Join Types at a Glance

| Join | Left | Right | Window | When to use |
|------|------|--------|--------|-------------|
| **Stream-stream** | KStream | KStream | Required (JoinWindows) | Correlate two event streams by key within a time window (e.g. order + payment within 5 min) |
| **Stream-table** | KStream | KTable | None | Enrich each stream event with current table value for that key (e.g. order + customer profile) |
| **Table-table** | KTable | KTable | None | Combine two tables keyed the same (e.g. customer + customer_preferences) |
| **Table-stream** | KTable | KStream | Required | Less common; stream triggers lookup into table |

**Key rule:** For a join, both sides must be **co-partitioned** by the same key. Use `selectKey` or `groupBy` before the join so keys align (e.g. order stream keyed by orderId → re-key by customerId to join with customer table).

---

## JoinWindows (Stream-Stream)

Stream-stream joins are **time-bounded**: two records join only if their timestamps are within a **time difference**. You define this with `JoinWindows`.

```java
import org.apache.kafka.streams.kstream.JoinWindows;

// Join if events are within 5 minutes of each other
JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5));

// 5-minute window, 1-minute grace for late-arriving events
JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1));
```

- **Time difference**: maximum gap between the two records’ timestamps (symmetric: order time ± 5 min).
- **Grace**: how long after the window you still accept late events (similar to windowed aggregations).

**Real use:** Order and payment must occur within 5 minutes; out-of-order payment within 1 minute after the window is still joined.

---

## Stream-Stream Joins: Inner, Left, Outer

All three variants take a **ValueJoiner**: `(leftValue, rightValue) -> result`. The window defines which pairs are considered.

### Inner join

Only emits when **both** sides have a record in the window. No record if only one side has data.

```java
KStream<String, Order> orders = builder.stream("orders", ...);
KStream<String, Payment> payments = builder.stream("payments", ...);

JoinWindows windows = JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofMinutes(1));

KStream<String, OrderWithPayment> joined = orders.join(
    payments,
    (order, payment) -> new OrderWithPayment(order, payment),
    windows
);
```

### Left join

Emits when the **left** side has a record; if the right has a match in the window, the value is present; otherwise right value is **null**.

```java
KStream<String, OrderWithPayment> leftJoined = orders.leftJoin(
    payments,
    (order, payment) -> new OrderWithPayment(order, payment),
    windows
);
// payment can be null when no payment in window
```

### Outer join

Emits when **either** side has a record; the other side’s value may be null.

```java
KStream<String, OrderWithPayment> outerJoined = orders.outerJoin(
    payments,
    (order, payment) -> new OrderWithPayment(order, payment),
    windows
);
```

**Real use:** Inner = “only orders that have a payment in time.” Left = “all orders; attach payment if present.” Outer = “all orders and all payments; pair when in window.”

---

## Stream-Table Join (Enrichment)

Each **stream** record is joined with the **current** value in the **table** for the same key. No window: the table is a point-in-time lookup. If there is no table entry for the key, the table value is **null** in the joiner (for stream-table left join; inner would drop).

```java
KStream<String, Order> orders = builder.stream("orders", ...);
KTable<String, Customer> customers = builder.table("customers", ...);

// Enrich order with customer (same key = e.g. customerId)
KStream<String, EnrichedOrder> enriched = orders.join(
    customers,
    (order, customer) -> new EnrichedOrder(order, customer),
    Joined.with(Serdes.String(), orderSerde, customerSerde)
);
```

- **Keys must match.** If orders are keyed by orderId, use `selectKey((k, order) -> order.getCustomerId())` before the join, then join with the customer table (keyed by customerId). You may need to re-key back to orderId after the join if downstream expects it.
- **Table side:** Always “latest value” for that key. No time window.

**Real use:** Enrich every order with the current customer profile (name, tier, region) for analytics or routing.

---

## Table-Table Join

Both sides are **KTables** (changelog streams). Join is **non-windowed**: when either table has an update for a key, the join is recomputed and can emit a new result. Inner vs left semantics apply (left = left table value + right table value or null).

```java
KTable<String, Customer> customers = builder.table("customers", ...);
KTable<String, CustomerPreferences> prefs = builder.table("customer-preferences", ...);

KTable<String, CustomerWithPrefs> joined = customers.join(
    prefs,
    (customer, pref) -> new CustomerWithPrefs(customer, pref)
);
```

**Real use:** Combine customer master data with preferences; any update to either table updates the joined view.

---

## Foreign-Key Table-Table Join

When the **table** key is not the same as the **stream/table** key (e.g. orders keyed by orderId, customer table keyed by customerId), you have a **foreign key** relationship. Kafka Streams supports this with **KTable-KTable foreign key join** (as of 2.4+): one table is the “primary,” the other is looked up by a key extracted from the primary value.

```java
// Orders by orderId; customer table by customerId
KTable<String, Order> ordersTable = builder.table("orders", ...);
KTable<String, Customer> customersTable = builder.table("customers", ...);

KTable<String, OrderWithCustomer> withCustomer = ordersTable.join(
    customersTable,
    order -> order.getCustomerId(),   // FK extractor: order -> customerId
    (order, customer) -> new OrderWithCustomer(order, customer),
    Materialized.as("orders-with-customer")
);
```

- Left table (orders) is scanned by its key (orderId).
- For each order, the **foreign key** (customerId) is computed; the right table (customers) is looked up by that key.
- Result is keyed by the **left** key (orderId).

**Real use:** Orders and customers in separate tables; join to get order + customer without re-keying the stream yourself.

---

## Null Keys and Null Values

- **Null key:** Records with a null key are **dropped** before the join; they never produce a result. Filter or map to a sentinel key if you must keep them.
- **Null value (table or stream):** In **left** join, the non-matching side is passed as **null** to the ValueJoiner. In **inner** join, a null from the other side usually means no emit (stream-stream: no match in window; stream-table: no table entry).
- **ValueJoiner:** Handle nulls inside the joiner (e.g. `customer != null ? customer.getName() : "unknown"`) so downstream gets a safe value.

```java
orders.leftJoin(customers, (order, customer) -> {
    return new EnrichedOrder(order, customer != null ? customer : Customer.unknown());
}, Joined.with(...));
```

---

## Real Use Cases Where Joins Add Value

| Use case | Why a join fits | Join type |
|----------|------------------|-----------|
| **Order + payment** | Confirm payment received within N minutes of order. | Stream-stream (inner/left), JoinWindows |
| **Order + customer** | Attach customer tier, region, or name to every order for routing or analytics. | Stream-table (orders re-keyed by customerId) |
| **Customer + preferences** | Single view of customer and their settings. | Table-table |
| **Order + customer (FK)** | Orders and customers in separate tables; need order with customer details. | Table-table FK join |
| **Event + reference data** | Enrich events with a lookup table (e.g. productId → product name). | Stream-table |

**Use case we implement:** **Order enrichment with customer data** — stream of orders re-keyed by customerId, joined with a customer KTable (or table topic), producing enriched orders. Implemented in the Day 5 demo.

---

## Implemented Use Case: Order Enrichment (Stream-Table)

The Day 5 demo implements: **orders** (stream) + **customers** (table) → **enriched orders** (stream). Keys align on customerId.

**Pipeline (conceptually):**

1. Read **orders** as KStream (key = orderId or any; value = order event with customerId).
2. **selectKey** so the key is customerId (for co-partitioning with the customer table).
3. Read **customers** as KTable (key = customerId, value = customer profile).
4. **Stream-table join** (left): order.join(customers, (order, customer) -> EnrichedOrder(order, customer)).
5. Optionally **selectKey** back to orderId if needed; then **to**("enriched-orders").

**Code (conceptual):** See Day5-Demo.

```java
KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));
KTable<String, Customer> customers = builder.table("customers", Consumed.with(Serdes.String(), customerSerde));

KStream<String, EnrichedOrder> enriched = orders
    .selectKey((orderId, order) -> order.getCustomerId())
    .join(
        customers,
        (order, customer) -> new EnrichedOrder(order, customer != null ? customer : Customer.unknown()),
        Joined.with(Serdes.String(), orderSerde, customerSerde)
    );
enriched.to("enriched-orders", Produced.with(Serdes.String(), enrichedOrderSerde));
```

**How to run:** Start the Day5-Demo app. Produce orders (with customerId) to `orders` and customer records (keyed by customerId) to `customers`. Consume `enriched-orders` to see order + customer per record.

---

## Best Practices

- **Co-partitioning:** Ensure both sides of a join use the same key and the same number of partitions for the join topic(s); otherwise use `groupBy`/repartition so they align.
- **Join window size (stream-stream):** Larger windows increase state and latency; smaller may miss valid pairs. Choose based on your SLA (e.g. “payment within 5 min”).
- **Grace period:** Set when you expect late or out-of-order events; otherwise use `ofTimeDifferenceWithNoGrace`.
- **Enrichment (stream-table):** Re-key the stream to the table key (e.g. customerId) before the join; re-key back after if the rest of the pipeline needs a different key.
- **Nulls:** Use **left** join when the right side might be missing; handle null in the ValueJoiner and avoid passing null downstream if possible.
- **Serdes:** Specify Serdes in `Joined.with(keySerde, leftValueSerde, rightValueSerde)` (and for Materialized in table-table) to avoid inference issues.

---

## Summary

| Join | Left | Right | Window | Key rule |
|------|------|--------|--------|----------|
| **Stream-stream** | KStream | KStream | JoinWindows required | Same key; time window for matching |
| **Stream-table** | KStream | KTable | None | Same key; table = lookup |
| **Table-table** | KTable | KTable | None | Same key |
| **Table-table FK** | KTable | KTable | None | FK extractor from left value to right key |
| **Inner** | Emit only when both match | | | |
| **Left** | Emit for every left; right can be null | | | |
| **Outer** | Emit when either has data; other can be null | | | |

**Next:** Day 6 — Interactive Queries, state store discovery, deployment and scaling.

---

## Resources

- [Kafka Streams – Joining](https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#joining)
- [JoinWindows](https://kafka.apache.org/34/javadoc/org/apache/kafka/streams/kstream/JoinWindows.html)
- [Confluent: Joins in Kafka Streams](https://developer.confluent.io/courses/kafka-streams/joins/)
