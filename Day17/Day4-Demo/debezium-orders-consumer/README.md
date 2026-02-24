# Debezium Orders Consumer - Day 4 Demo

Minimal Spring Boot consumer that reads Debezium change events from the orders table.

## What It Does

- Consumes from topic: `mysql-server.ecommerce_db.orders`
- Reads Debezium change events (with unwrap transform applied)
- Handles CREATE, UPDATE, and DELETE operations
- Logs all change events

## Running the Consumer

### Prerequisites

1. Day 4 demo environment running (Docker Compose)
2. Debezium connector registered and running
3. Java 21 installed

### Build and Run

```bash
# Build the project
./gradlew build

# Run the consumer
./gradlew bootRun
```

Or using Maven (if you prefer):

```bash
mvn spring-boot:run
```

## Configuration

The consumer is configured in `application.properties`:
- Kafka bootstrap servers: `localhost:9092`
- Consumer group: `debezium-orders-consumer-group`
- Topic: `mysql-server.ecommerce_db.orders`

## Testing

1. Make changes to the orders table in MySQL:
```bash
# Insert new order
docker-compose exec mysql mysql -uroot -prootpassword ecommerce_db -e "INSERT INTO orders (customer_id, product_id, quantity, price, order_status) VALUES (1006, 2005, 1, 49.99, 'PENDING');"

# Update order
docker-compose exec mysql mysql -uroot -prootpassword ecommerce_db -e "UPDATE orders SET order_status = 'SHIPPED' WHERE order_id = 1;"

# Delete order
docker-compose exec mysql mysql -uroot -prootpassword ecommerce_db -e "DELETE FROM orders WHERE order_id = 5;"
```

2. Watch the consumer logs - you should see change events appear in real-time!

## Message Format

With the unwrap transform, messages are Avro GenericRecord format.
The consumer uses `GenericRecord` to read the data without needing to generate Avro classes.

The record contains:
- `order_id`, `customer_id`, `product_id`, `quantity`, `price`, `order_status`
- `created_at`, `updated_at`
- `op` (operation: c=create, u=update, d=delete)
- `source.ts_ms` (timestamp)

The `op` field indicates the operation:
- `c` = CREATE (insert)
- `u` = UPDATE
- `d` = DELETE
- `r` = READ (initial snapshot)

## Requirements

- Java 21
- Gradle 8.5+ (or use Gradle wrapper)
- Schema Registry running on http://localhost:8081
- Kafka running on localhost:9092

