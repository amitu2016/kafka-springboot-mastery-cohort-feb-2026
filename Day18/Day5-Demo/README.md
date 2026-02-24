# Day 5 Demo: Elasticsearch & Search Integration

This demo shows how to use the Elasticsearch Sink Connector to index Kafka events into Elasticsearch for search and analytics.

## Prerequisites

- Docker and Docker Compose installed
- Basic understanding of Elasticsearch concepts

## Quick Start

### 1. Start Services

```bash
cd Day5-Demo
docker compose up -d
```

Wait for all services to be healthy (especially Elasticsearch):

```bash
# Check Elasticsearch
curl http://localhost:9200

# Check Kafka Connect
curl http://localhost:8083/connectors

# Check Schema Registry
curl http://localhost:8081/subjects
```

### 2. Create JDBC Source Connector

This will read orders from PostgreSQL and publish to Kafka:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @jdbc-source-orders.json
```

Verify it's running:

```bash
curl http://localhost:8083/connectors/jdbc-source-orders/status
```

### 3. Create Elasticsearch Sink Connector

This will consume from Kafka and index to Elasticsearch:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @elasticsearch-sink-orders.json
```

Verify it's running:

```bash
curl http://localhost:8083/connectors/elasticsearch-sink-orders/status
```

### 4. Verify Indexing

**Check Elasticsearch indices**:

```bash
curl http://localhost:9200/_cat/indices?v
```

**Search documents**:

```bash
curl http://localhost:9200/db-orders/_search?pretty
```

**Get specific document**:

```bash
curl http://localhost:9200/db-orders/_doc/1?pretty
```

### 5. Add More Data

Insert more orders into PostgreSQL:

```bash
docker exec -it postgres psql -U kafka_user -d ecommerce_db -c \
  "INSERT INTO orders (customer_id, product_id, quantity, price, order_status) VALUES (1009, 2006, 2, 199.98, 'PENDING');"
```

Wait a few seconds, then check Elasticsearch again - the new order should be indexed.

## Alternative Configurations

### Use Document ID from Record Value

This configuration extracts the document ID from the `order_id` field in the record value, enabling idempotent updates:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @elasticsearch-sink-orders-with-id.json
```

### Enable Dead Letter Queue

This configuration enables error tolerance and DLQ for failed records:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @elasticsearch-sink-orders-with-dlq.json
```

## Access UIs

- **Kafdrop**: http://localhost:9000 (Kafka UI)
- **Elasticsearch**: http://localhost:9200 (REST API)
- **Kafka Connect REST API**: http://localhost:8083

## Clean Up

```bash
# Stop and remove containers
docker compose down

# Remove volumes (deletes all data)
docker compose down -v
```

## Troubleshooting

### Elasticsearch not responding

Check Elasticsearch logs:

```bash
docker logs elasticsearch
```

### Connector failing

Check connector status and logs:

```bash
curl http://localhost:8083/connectors/elasticsearch-sink-orders/status
docker logs kafka-connect
```

### No documents in Elasticsearch

1. Verify JDBC source connector is running and producing data
2. Check Kafka topic has messages: `curl http://localhost:9000` (Kafdrop)
3. Check Elasticsearch sink connector status
4. Verify Elasticsearch is healthy: `curl http://localhost:9200/_cluster/health`

## Next Steps

- Explore Elasticsearch queries and aggregations
- Set up Kibana for visualization
- Experiment with different batch sizes
- Try custom index mappings
- Monitor connector metrics

